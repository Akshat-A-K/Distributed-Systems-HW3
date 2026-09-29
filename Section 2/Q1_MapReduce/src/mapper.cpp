// Hadoop Streaming mapper for HW3 Section 2 Q1 (HW2 Q8 weather analytics).
//
// Contract: reads pure data records from stdin, one per line --
//   timestamp station_id temperature humidity pressure rainfall wind_speed
// NOT the "N K S" header line from HW2's file format. Stripping that header
// (and carrying K forward to finalize.py, which is the only piece that
// needs it for the top-K cutoff) is the driving script's job, not this
// program's -- a real Hadoop input split never contains anything but data
// records, and this mapper is written to that same assumption.
//
// Accumulates LOCAL state only, across its entire input (never emits one
// line per record -- see the header comment in kahan.hpp for why one
// mapper process handling many lines sequentially is expected to hold
// running state, exactly like one MPI rank's local loop in weather_mpi.cpp).
// At EOF, emits three kinds of summary lines:
//   GLOBAL                 -- exactly one per mapper (this split's contribution
//                              to every Shape-A statistic, see plan section 4)
//   INTERVAL_<interval_id> -- one per distinct interval_id seen in this split
//   STATION_<station_id>   -- one per distinct station_id seen in this split
//
// Output format: "key<TAB>value\n" for every emitted line, per the Hadoop
// Streaming contract. Keys never contain spaces (STATION_/INTERVAL_ ids are
// integers). Within a value, fields are separated by a single space -- safe
// and unambiguous because every field is numeric and none can itself
// contain whitespace, and because each value has a fixed, documented field
// count read back with operator>>, which is insensitive to how many spaces
// separate tokens.
//
// Numerical behavior and tie-break rules are preserved exactly from
// weather_seq_reference/weather_seq.cpp: same Kahan summation (via
// common/kahan.hpp), same extreme-event predicate, same hottest/coldest
// comparator (max/min temperature; tie -> smaller timestamp; tie -> smaller
// station_id), same interval_id = timestamp / 60.

#include "common/kahan.hpp"

#include <iomanip>
#include <iostream>
#include <limits>
#include <unordered_map>

using namespace std;

struct StationLocal {
    long long count = 0;
    KahanSum sum_temp;
    KahanSum sum_rainfall;
};

int main() {
    ios_base::sync_with_stdio(false);
    cin.tie(nullptr);

    // max_digits10 (17 for double) is the number of significant decimal
    // digits that guarantees a double round-trips exactly through text --
    // required here because these are intermediate values the reducer will
    // parse back and combine further, not final 2-decimal display output.
    cout << setprecision(numeric_limits<double>::max_digits10);

    bool has_data = false;
    long long count = 0;

    KahanSum sum_temp, sum_humidity, sum_pressure, sum_rainfall, sum_wind;

    // No sentinel values: min/max are only meaningful once at least one
    // record has been seen, so they're set directly from the first record
    // rather than compared against an arbitrary large/small placeholder.
    double min_temp = 0.0, max_temp = 0.0;
    double min_humidity = 0.0, max_humidity = 0.0;
    double min_pressure = 0.0, max_pressure = 0.0;
    double max_rainfall = 0.0;
    double max_wind = 0.0;

    long long extreme_count = 0;

    bool have_hottest = false, have_coldest = false;
    double hottest_temp = 0.0;
    long long hottest_ts = 0;
    int hottest_station = 0;
    double coldest_temp = 0.0;
    long long coldest_ts = 0;
    int coldest_station = 0;

    unordered_map<int, StationLocal> station_map;
    unordered_map<long long, long long> interval_map;

    long long ts;
    int station_id;
    double temp, hum, pres, rain, wind;

    while (cin >> ts >> station_id >> temp >> hum >> pres >> rain >> wind) {
        has_data = true;
        ++count;

        sum_temp.add(temp);
        sum_humidity.add(hum);
        sum_pressure.add(pres);
        sum_rainfall.add(rain);
        sum_wind.add(wind);

        if (count == 1) {
            min_temp = max_temp = temp;
            min_humidity = max_humidity = hum;
            min_pressure = max_pressure = pres;
            max_rainfall = rain;
            max_wind = wind;
        } else {
            min_temp = min(min_temp, temp);
            max_temp = max(max_temp, temp);
            min_humidity = min(min_humidity, hum);
            max_humidity = max(max_humidity, hum);
            min_pressure = min(min_pressure, pres);
            max_pressure = max(max_pressure, pres);
            max_rainfall = max(max_rainfall, rain);
            max_wind = max(max_wind, wind);
        }

        if (temp >= 40.0 || temp <= 0.0) {
            ++extreme_count;
        }

        // Hottest: max temperature; tie -> smaller timestamp; tie -> smaller station_id.
        if (!have_hottest || temp > hottest_temp ||
            (temp == hottest_temp && ts < hottest_ts) ||
            (temp == hottest_temp && ts == hottest_ts && station_id < hottest_station)) {
            hottest_temp = temp;
            hottest_ts = ts;
            hottest_station = station_id;
            have_hottest = true;
        }

        // Coldest: min temperature; tie -> smaller timestamp; tie -> smaller station_id.
        if (!have_coldest || temp < coldest_temp ||
            (temp == coldest_temp && ts < coldest_ts) ||
            (temp == coldest_temp && ts == coldest_ts && station_id < coldest_station)) {
            coldest_temp = temp;
            coldest_ts = ts;
            coldest_station = station_id;
            have_coldest = true;
        }

        StationLocal &sl = station_map[station_id];
        ++sl.count;
        sl.sum_temp.add(temp);
        sl.sum_rainfall.add(rain);

        long long interval_id = ts / 60;
        ++interval_map[interval_id];
    }

    // Empty split: nothing valid to report. Emitting a GLOBAL line here
    // would force every consumer to special-case a count==0 partial with
    // meaningless min/max/hottest/coldest fields -- simpler and safer to
    // contribute nothing at all when there is nothing to contribute.
    if (!has_data) {
        return 0;
    }

    // GLOBAL value: 21 space-separated fields, always in this exact order:
    //   count
    //   sum_temp sum_humidity sum_pressure sum_rainfall sum_wind
    //   min_temp max_temp min_humidity max_humidity min_pressure max_pressure
    //   max_rainfall max_wind
    //   extreme_count
    //   hottest_temp hottest_timestamp hottest_station_id
    //   coldest_temp coldest_timestamp coldest_station_id
    cout << "GLOBAL\t"
         << count << ' '
         << sum_temp.value() << ' ' << sum_humidity.value() << ' ' << sum_pressure.value() << ' '
         << sum_rainfall.value() << ' ' << sum_wind.value() << ' '
         << min_temp << ' ' << max_temp << ' '
         << min_humidity << ' ' << max_humidity << ' '
         << min_pressure << ' ' << max_pressure << ' '
         << max_rainfall << ' ' << max_wind << ' '
         << extreme_count << ' '
         << hottest_temp << ' ' << hottest_ts << ' ' << hottest_station << ' '
         << coldest_temp << ' ' << coldest_ts << ' ' << coldest_station
         << '\n';

    // STATION_<id> value: "count sum_temp sum_rainfall".
    for (const auto &entry : station_map) {
        cout << "STATION_" << entry.first << '\t'
             << entry.second.count << ' '
             << entry.second.sum_temp.value() << ' '
             << entry.second.sum_rainfall.value()
             << '\n';
    }

    // INTERVAL_<id> value: local count for that interval.
    for (const auto &entry : interval_map) {
        cout << "INTERVAL_" << entry.first << '\t' << entry.second << '\n';
    }

    return 0;
}
