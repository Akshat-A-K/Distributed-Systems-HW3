#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
using namespace std;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cout << "Usage: " << argv[0] << " <input_file>" << endl;
        return 1;
    }
    ifstream file(argv[1]);
    if (!file) {
        cout << "Error opening file: " << argv[1] << endl;
        return 1;
    }
    int n, p;
    file >> n >> p;
    vector<vector<long long>> b(n, vector<long long>(p));
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < p; ++j) {
            file >> b[i][j];
        }
    }
    string line;
    while (getline(cin, line)) {
        if (line.empty()) {
            continue;
        }
        stringstream input(line);
        int row;
        input >> row;
        vector<long long> a(n);
        for (int i = 0; i < n; ++i) {
            input >> a[i];
        }
        vector<long long> ans(p, 0);
        for (int k = 0; k < n; k++) {
            for (int j = 0; j < p; j++) {
                ans[j] += a[k] * b[k][j];
            }
        }
        cout << row << "\t";
        for (int j = 0; j < p; j++) {
            cout << ans[j] << " ";
        }
        cout << endl;
    }
    return 0;
}