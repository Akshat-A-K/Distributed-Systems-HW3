#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

struct Record {
    long long timestamp;
    int station_id;
    double temperature;
    double humidity;
    double pressure;
    double rainfall;
    double wind_speed;
};

// Kahan (compensated) summation: tracks a running correction term so that
// small rounding errors from each addition don't silently accumulate.
// Plain "sum += x" repeated N times can drift by O(N * epsilon); this keeps
// the error close to O(epsilon) regardless of N. This matters here because
// the MPI version sums in a different grouping (local chunks combined via
// reduction) than this sequential single loop -- without compensation the
// two can disagree in the last printed decimal digit.
struct KahanSum {
    long double sum = 0.0L;
    long double c = 0.0L;
    void add(double x_in) {
        long double x = (long double)x_in;
        long double y = x - c;
        long double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
};

struct StationAgg {
    long long count = 0;
    KahanSum sum_temp;
    KahanSum sum_rainfall;
};

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " <filename>" << endl;
        return 1;
    }

    ifstream infile(argv[1]);
    if (!infile) {
        cerr << "Error opening file: " << argv[1] << endl;
        return 1;
    }

    long long n, k, s;
    if (!(infile >> n >> k >> s)) {
        cerr << "Error reading N K S header." << endl;
        return 1;
    }

    vector<Record> records(n);
    for (long long i = 0; i < n; ++i) {
        Record &r = records[i];
        if (!(infile >> r.timestamp >> r.station_id >> r.temperature >> r.humidity >>
              r.pressure >> r.rainfall >> r.wind_speed)) {
            cerr << "Error reading record at index " << i << endl;
            return 1;
        }
    }
    infile.close();

    KahanSum sum_temp, sum_humidity, sum_pressure, sum_rainfall, sum_wind;
    double min_temp = 1e300, max_temp = -1e300;
    double min_humidity = 1e300, max_humidity = -1e300;
    double min_pressure = 1e300, max_pressure = -1e300;
    double max_rainfall = -1e300;
    double max_wind = -1e300;
    long long extreme_count = 0;

    bool have_hottest = false, have_coldest = false;
    Record hottest{}, coldest{};

    unordered_map<int, StationAgg> station_map;
    unordered_map<long long, long long> interval_map;

    for (long long i = 0; i < n; ++i) {
        const Record &r = records[i];

        sum_temp.add(r.temperature);
        min_temp = min(min_temp, r.temperature);
        max_temp = max(max_temp, r.temperature);

        sum_humidity.add(r.humidity);
        min_humidity = min(min_humidity, r.humidity);
        max_humidity = max(max_humidity, r.humidity);

        sum_pressure.add(r.pressure);
        min_pressure = min(min_pressure, r.pressure);
        max_pressure = max(max_pressure, r.pressure);

        sum_rainfall.add(r.rainfall);
        max_rainfall = max(max_rainfall, r.rainfall);

        sum_wind.add(r.wind_speed);
        max_wind = max(max_wind, r.wind_speed);

        if (r.temperature >= 40.0 || r.temperature <= 0.0) {
            extreme_count++;
        }

        // Hottest: max temperature; tie -> smaller timestamp; tie -> smaller station_id
        if (!have_hottest || r.temperature > hottest.temperature ||
            (r.temperature == hottest.temperature && r.timestamp < hottest.timestamp) ||
            (r.temperature == hottest.temperature && r.timestamp == hottest.timestamp &&
             r.station_id < hottest.station_id)) {
            hottest = r;
            have_hottest = true;
        }

        // Coldest: min temperature; tie -> smaller timestamp; tie -> smaller station_id
        if (!have_coldest || r.temperature < coldest.temperature ||
            (r.temperature == coldest.temperature && r.timestamp < coldest.timestamp) ||
            (r.temperature == coldest.temperature && r.timestamp == coldest.timestamp &&
             r.station_id < coldest.station_id)) {
            coldest = r;
            have_coldest = true;
        }

        StationAgg &sa = station_map[r.station_id];
        sa.count++;
        sa.sum_temp.add(r.temperature);
        sa.sum_rainfall.add(r.rainfall);

        long long interval_id = r.timestamp / 60;
        interval_map[interval_id]++;
    }

    double avg_temp = sum_temp.sum / n;
    double avg_humidity = sum_humidity.sum / n;
    double avg_pressure = sum_pressure.sum / n;
    double avg_wind = sum_wind.sum / n;

    // Busiest interval: highest count; tie -> smaller interval_id
    long long busiest_interval = -1, busiest_count = -1;
    for (const auto &entry : interval_map) {
        if (entry.second > busiest_count ||
            (entry.second == busiest_count && entry.first < busiest_interval)) {
            busiest_count = entry.second;
            busiest_interval = entry.first;
        }
    }

    // Top-K stations: count desc, station_id asc
    vector<pair<int, StationAgg>> stations(station_map.begin(), station_map.end());
    sort(stations.begin(), stations.end(), [](const auto &a, const auto &b) {
        if (a.second.count != b.second.count) return a.second.count > b.second.count;
        return a.first < b.first;
    });

    // Precision updated from HW2's original 2 decimals to 6, per the course
    // clarification for Q7/Q8 received during HW3 Section 2: "All
    // floating-point values in the output should be printed with exactly 6
    // digits after the decimal point." This is a display-formatting change
    // only -- no algorithm/logic above this line was touched -- kept so
    // this file remains a faithful correctness oracle for the now-clarified
    // spec (matching the precision reducer.cpp/finalize.py were also
    // updated to use), not to make a buggy Hadoop output match on purpose.
    cout << fixed << setprecision(6);
    cout << "TOTAL_MEASUREMENTS " << n << "\n";
    cout << "AVERAGE_TEMPERATURE " << avg_temp << "\n";
    cout << "MIN_TEMPERATURE " << min_temp << "\n";
    cout << "MAX_TEMPERATURE " << max_temp << "\n";
    cout << "AVERAGE_HUMIDITY " << avg_humidity << "\n";
    cout << "MIN_HUMIDITY " << min_humidity << "\n";
    cout << "MAX_HUMIDITY " << max_humidity << "\n";
    cout << "AVERAGE_PRESSURE " << avg_pressure << "\n";
    cout << "MIN_PRESSURE " << min_pressure << "\n";
    cout << "MAX_PRESSURE " << max_pressure << "\n";
    cout << "TOTAL_RAINFALL " << sum_rainfall.sum << "\n";
    cout << "MAX_RAINFALL " << max_rainfall << "\n";
    cout << "AVERAGE_WIND_SPEED " << avg_wind << "\n";
    cout << "MAX_WIND_SPEED " << max_wind << "\n";
    cout << "EXTREME_TEMPERATURE_EVENTS " << extreme_count << "\n";
    cout << "HOTTEST_MEASUREMENT " << hottest.temperature << " " << hottest.station_id << " "
         << hottest.timestamp << "\n";
    cout << "COLDEST_MEASUREMENT " << coldest.temperature << " " << coldest.station_id << " "
         << coldest.timestamp << "\n";
    cout << "BUSIEST_INTERVAL " << busiest_interval << " " << busiest_count << "\n";
    cout << "TOP_STATIONS\n";
    long long shown = min<long long>(k, (long long)stations.size());
    for (long long i = 0; i < shown; ++i) {
        const auto &st = stations[i];
        double station_avg_temp = st.second.sum_temp.sum / st.second.count;
        cout << st.first << " " << st.second.count << " " << station_avg_temp << " "
             << st.second.sum_rainfall.sum << "\n";
    }

    return 0;
}
