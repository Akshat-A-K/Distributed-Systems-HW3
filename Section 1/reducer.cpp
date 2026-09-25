#include <iostream>
#include <sstream>
#include <vector>
using namespace std;

int main() {
    string line;
    int current = -1;
    vector<long long> sum;
    while (getline(cin, line)) {
        if (line.empty())
            continue;
        stringstream input(line);
        int row;
        input >> row;
        vector<long long> values;
        long long x;
        while (input >> x)
            values.push_back(x);
        if (current == -1) {
            current = row;
            sum = values;
        }
        else if (row == current) {
            for (int i = 0; i < values.size(); i++)
                sum[i] += values[i];
        }
        else {
            for (int i = 0; i < sum.size(); i++)
                cout << (i == 0 ? "" : " ") << sum[i];
            cout << endl;
            current = row;
            sum = values;
        }
    }
    if (current != -1) {
        for (int i = 0; i < sum.size(); i++)
            cout << (i == 0 ? "" : " ") << sum[i];
        cout << endl;
    }
    return 0;
}