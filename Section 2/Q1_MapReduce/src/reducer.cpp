// Hadoop Streaming reducer for HW3 Section 2 Q1 (HW2 Q8 weather analytics).
//
// Contract: reads the shuffle-sorted "key<TAB>value" stream from stdin --
// same-key lines are adjacent (guaranteed by the sort), but NOT pre-grouped
// into lists, so this reducer detects key-boundary changes itself, exactly
// like the classic Hadoop Streaming reducer pattern.
//
// Three key types, three combine strategies (see plan section 5):
//
//   GLOBAL          -- always routes to exactly one reducer instance, for
//                       any number of reduce tasks, because it's a single
//                       constant key. That reducer therefore sees every
//                       mapper's GLOBAL partial and can finish Shape A
//                       completely here: one more Kahan pass over each
//                       partial's already-compensated sum (not a plain
//                       running += over them -- see kahan.hpp), plain
//                       min/max, and the same hottest/coldest comparator as
//                       weather_seq.cpp, re-applied across the mappers'
//                       local candidates. Because nothing further needs to
//                       happen to Shape A, this reducer prints it directly
//                       in FINAL Q8 format (fixed, 2 decimals) -- this is
//                       the one place in the whole pipeline where output
//                       switches from round-trip-safe intermediate
//                       precision to final display precision.
//
//   STATION_<id>    -- one interval/station id always routes to the same
//   INTERVAL_<id>      reducer instance (same reasoning as GLOBAL, just per
//                       distinct key rather than one global key), so this
//                       reducer sees every mapper's partial for that
//                       specific id and can sum them into a COMPLETE total
//                       for that id. Still intermediate output, though --
//                       finding the single busiest interval and sorting
//                       stations for top-K needs a view across ALL
//                       interval/station ids, which this one reducer
//                       instance does not have if there is more than one
//                       reduce task (different ids can land on different
//                       reducers). That global view is finalize.py's job,
//                       not this program's -- see plan section 5.
//
// Key sort order note: Hadoop's default sort is lexicographic on the key
// string, so e.g. "INTERVAL_10" sorts before "INTERVAL_2". This does not
// affect correctness here -- all that's required is that lines sharing the
// exact same key end up adjacent, which lexicographic sort still guarantees
// regardless of numeric order between *different* keys. finalize.py does
// its own proper numeric comparison later; nothing in this reducer relies
// on interval/station ids arriving in numeric order.

#include "common/kahan.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

using namespace std;

// Accumulates the single GLOBAL group across every mapper partial it sees.
struct GlobalCombiner {
    long long count = 0;
    KahanSum sum_temp, sum_humidity, sum_pressure, sum_rainfall, sum_wind;

    bool have_bounds = false;
    double min_temp = 0.0, max_temp = 0.0;
    double min_humidity = 0.0, max_humidity = 0.0;
    double min_pressure = 0.0, max_pressure = 0.0;
    double max_rainfall = 0.0, max_wind = 0.0;

    long long extreme_count = 0;

    bool have_hottest = false, have_coldest = false;
    double hottest_temp = 0.0;
    long long hottest_ts = 0;
    int hottest_station = 0;
    double coldest_temp = 0.0;
    long long coldest_ts = 0;
    int coldest_station = 0;

    // Parses one mapper's GLOBAL value (21 fields, in the exact order
    // mapper.cpp documents and emits) and folds it into the running merge.
    void merge_one(istream &in) {
        long long p_count;
        double p_sum_temp, p_sum_hum, p_sum_pres, p_sum_rain, p_sum_wind;
        double p_min_t, p_max_t, p_min_h, p_max_h, p_min_p, p_max_p;
        double p_max_rain, p_max_wind;
        long long p_extreme;
        double p_hot_t;
        long long p_hot_ts;
        int p_hot_st;
        double p_cold_t;
        long long p_cold_ts;
        int p_cold_st;

        in >> p_count >> p_sum_temp >> p_sum_hum >> p_sum_pres >> p_sum_rain >> p_sum_wind >>
            p_min_t >> p_max_t >> p_min_h >> p_max_h >> p_min_p >> p_max_p >> p_max_rain >>
            p_max_wind >> p_extreme >> p_hot_t >> p_hot_ts >> p_hot_st >> p_cold_t >> p_cold_ts >>
            p_cold_st;

        count += p_count;
        sum_temp.add(p_sum_temp);
        sum_humidity.add(p_sum_hum);
        sum_pressure.add(p_sum_pres);
        sum_rainfall.add(p_sum_rain);
        sum_wind.add(p_sum_wind);

        if (!have_bounds) {
            min_temp = p_min_t;
            max_temp = p_max_t;
            min_humidity = p_min_h;
            max_humidity = p_max_h;
            min_pressure = p_min_p;
            max_pressure = p_max_p;
            max_rainfall = p_max_rain;
            max_wind = p_max_wind;
            have_bounds = true;
        } else {
            min_temp = min(min_temp, p_min_t);
            max_temp = max(max_temp, p_max_t);
            min_humidity = min(min_humidity, p_min_h);
            max_humidity = max(max_humidity, p_max_h);
            min_pressure = min(min_pressure, p_min_p);
            max_pressure = max(max_pressure, p_max_p);
            max_rainfall = max(max_rainfall, p_max_rain);
            max_wind = max(max_wind, p_max_wind);
        }

        extreme_count += p_extreme;

        // Same hottest/coldest comparator as weather_seq.cpp, re-applied
        // across mapper-local candidates instead of individual records.
        if (!have_hottest || p_hot_t > hottest_temp ||
            (p_hot_t == hottest_temp && p_hot_ts < hottest_ts) ||
            (p_hot_t == hottest_temp && p_hot_ts == hottest_ts && p_hot_st < hottest_station)) {
            hottest_temp = p_hot_t;
            hottest_ts = p_hot_ts;
            hottest_station = p_hot_st;
            have_hottest = true;
        }
        if (!have_coldest || p_cold_t < coldest_temp ||
            (p_cold_t == coldest_temp && p_cold_ts < coldest_ts) ||
            (p_cold_t == coldest_temp && p_cold_ts == coldest_ts && p_cold_st < coldest_station)) {
            coldest_temp = p_cold_t;
            coldest_ts = p_cold_ts;
            coldest_station = p_cold_st;
            have_coldest = true;
        }
    }

    // Prints the 15 Shape-A fields in final Q8 format. Not called unless
    // count > 0 (see main()), so the average divisions below are safe.
    //
    // Precision: 6 digits after the decimal point, per the course
    // clarification for Q7/Q8 ("All floating-point values in the output
    // should be printed with exactly 6 digits after the decimal point").
    void print_final(ostream &out) const {
        out << fixed << setprecision(6);
        out << "TOTAL_MEASUREMENTS " << count << "\n";
        out << "AVERAGE_TEMPERATURE " << (sum_temp.value() / static_cast<double>(count)) << "\n";
        out << "MIN_TEMPERATURE " << min_temp << "\n";
        out << "MAX_TEMPERATURE " << max_temp << "\n";
        out << "AVERAGE_HUMIDITY " << (sum_humidity.value() / static_cast<double>(count)) << "\n";
        out << "MIN_HUMIDITY " << min_humidity << "\n";
        out << "MAX_HUMIDITY " << max_humidity << "\n";
        out << "AVERAGE_PRESSURE " << (sum_pressure.value() / static_cast<double>(count)) << "\n";
        out << "MIN_PRESSURE " << min_pressure << "\n";
        out << "MAX_PRESSURE " << max_pressure << "\n";
        out << "TOTAL_RAINFALL " << sum_rainfall.value() << "\n";
        out << "MAX_RAINFALL " << max_rainfall << "\n";
        out << "AVERAGE_WIND_SPEED " << (sum_wind.value() / static_cast<double>(count)) << "\n";
        out << "MAX_WIND_SPEED " << max_wind << "\n";
        out << "EXTREME_TEMPERATURE_EVENTS " << extreme_count << "\n";
        out << "HOTTEST_MEASUREMENT " << hottest_temp << " " << hottest_station << " "
            << hottest_ts << "\n";
        out << "COLDEST_MEASUREMENT " << coldest_temp << " " << coldest_station << " "
            << coldest_ts << "\n";
    }
};

// Accumulates one STATION_<id> group: complete count/sum_temp/sum_rainfall
// for that station, across however many mappers saw it. Still intermediate
// -- finalize.py ranks stations against each other later.
struct StationCombiner {
    long long count = 0;
    KahanSum sum_temp, sum_rainfall;

    void merge_one(istream &in) {
        long long c;
        double st, sr;
        in >> c >> st >> sr;
        count += c;
        sum_temp.add(st);
        sum_rainfall.add(sr);
    }

    void print(ostream &out, const string &station_id) const {
        out << "STATION_" << station_id << '\t' << setprecision(numeric_limits<double>::max_digits10)
            << count << ' ' << sum_temp.value() << ' ' << sum_rainfall.value() << '\n';
    }
};

// Accumulates one INTERVAL_<id> group: complete count for that interval.
// Still intermediate -- finalize.py finds the global max later.
struct IntervalCombiner {
    long long count = 0;

    void merge_one(istream &in) {
        long long c;
        in >> c;
        count += c;
    }

    void print(ostream &out, const string &interval_id) const {
        out << "INTERVAL_" << interval_id << '\t' << count << '\n';
    }
};

int main() {
    ios_base::sync_with_stdio(false);
    cin.tie(nullptr);

    string line;
    string current_key;
    bool have_current_key = false;

    GlobalCombiner global_combiner;
    StationCombiner station_combiner;
    IntervalCombiner interval_combiner;

    // Prints and resets whichever STATION_*/INTERVAL_* group just finished.
    // GLOBAL is deliberately not flushed here -- it accumulates across the
    // whole input and is printed once, after the loop (see below).
    auto flush_current_group = [&]() {
        if (!have_current_key) return;
        if (current_key.rfind("STATION_", 0) == 0) {
            station_combiner.print(cout, current_key.substr(8));
            station_combiner = StationCombiner();
        } else if (current_key.rfind("INTERVAL_", 0) == 0) {
            interval_combiner.print(cout, current_key.substr(9));
            interval_combiner = IntervalCombiner();
        }
    };

    while (getline(cin, line)) {
        if (line.empty()) continue;

        size_t tab_pos = line.find('\t');
        string key = line.substr(0, tab_pos);
        string value = line.substr(tab_pos + 1);

        if (key != current_key) {
            flush_current_group();
            current_key = key;
            have_current_key = true;
        }

        istringstream value_stream(value);
        if (key == "GLOBAL") {
            global_combiner.merge_one(value_stream);
        } else if (key.rfind("STATION_", 0) == 0) {
            station_combiner.merge_one(value_stream);
        } else if (key.rfind("INTERVAL_", 0) == 0) {
            interval_combiner.merge_one(value_stream);
        }
    }
    flush_current_group();

    if (global_combiner.count > 0) {
        global_combiner.print_final(cout);
    }

    return 0;
}
