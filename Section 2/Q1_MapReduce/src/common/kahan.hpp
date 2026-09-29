#pragma once

// Kahan (compensated) summation.
//
// Naive running summation ("sum += x") accumulates floating-point rounding
// error that grows with the number of terms added. Kahan summation tracks a
// small correction term (`c`) alongside the running sum so the error stays
// roughly constant regardless of how many values are added. This matters
// here because the mapper sums each of its local records in one grouping,
// the reducer then combines multiple mappers' partial sums in whatever
// order the shuffle happens to deliver them, and a query later still needs
// to agree with HW2's single-pass sequential sum to within tolerance --
// without compensation, these different summation orders can disagree in
// the last printed decimal digit even though every input value is correct.
//
// The accumulation formula and the long double storage are preserved
// exactly from HW2's weather_seq.cpp / weather_mpi.cpp KahanSum struct.
// reset()/value() are additions on top of that unchanged core, giving a
// clearer construct/accumulate/read lifecycle for reuse in mapper.cpp and
// reducer.cpp without changing how any single value is summed.
struct KahanSum {
    long double sum = 0.0L;
    long double c = 0.0L;

    // Returns the accumulator to its initial (empty-sum) state.
    void reset() {
        sum = 0.0L;
        c = 0.0L;
    }

    // Adds one value to the running compensated sum. When the reducer needs
    // to combine another KahanSum's already-compensated partial (e.g. one
    // mapper's local sum) into this one, call add(other.value()) -- summing
    // the partials through this same function, rather than a plain running
    // += over them, is what keeps that second-level combine compensated too.
    void add(double x_in) {
        long double x = static_cast<long double>(x_in);
        long double y = x - c;
        long double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }

    // Current sum, as a double (the precision Q8's output format needs).
    // The compensation itself is still carried out internally in long double.
    double value() const {
        return static_cast<double>(sum);
    }
};
