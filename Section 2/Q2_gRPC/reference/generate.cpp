#include <iomanip>
#include <iostream>
#include <random>
#include <string>

using namespace std;

int main(int argc, char *argv[]) {
    ios_base::sync_with_stdio(false);
    cin.tie(NULL);

    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <N> [K] [S] [seed]" << endl;
        return 1;
    }

    long long n = stoll(argv[1]);
    long long k = (argc >= 3) ? stoll(argv[2]) : 5LL;
    long long s = (argc >= 4) ? stoll(argv[3]) : 100LL;
    unsigned long long seed = (argc >= 5) ? stoull(argv[4]) : 42ULL;

    mt19937_64 rng(seed);
    uniform_int_distribution<long long> ts_dist(1700000000LL, 1700086400LL);
    uniform_int_distribution<int> station_dist(1, (int)s);
    uniform_real_distribution<double> temp_dist(-10.0, 50.0);
    uniform_real_distribution<double> hum_dist(10.0, 100.0);
    uniform_real_distribution<double> press_dist(950.0, 1050.0);
    uniform_real_distribution<double> rain_dist(0.0, 50.0);
    uniform_real_distribution<double> wind_dist(0.0, 120.0);

    cout << fixed << setprecision(2);
    cout << n << " " << k << " " << s << "\n";

    for (long long i = 0; i < n; ++i) {
        long long ts = ts_dist(rng);
        int station = station_dist(rng);
        double temp = temp_dist(rng);
        double hum = hum_dist(rng);
        double press = press_dist(rng);
        double rain = rain_dist(rng);
        double wind = wind_dist(rng);

        cout << ts << " " << station << " " << temp << " " << hum << " "
             << press << " " << rain << " " << wind << "\n";
    }

    return 0;
}
