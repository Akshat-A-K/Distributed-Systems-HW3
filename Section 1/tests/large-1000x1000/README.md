# Large 1000 x 1000 Test

This case uses deterministic identity matrices:

- `A`: 1000 x 1000 identity matrix
- `B`: 1000 x 1000 identity matrix
- Expected result: 1000 x 1000 identity matrix

The data files are generated locally because storing them in Git is unnecessary. Run:

```powershell
.\generate_large_test.ps1
.\run_large_test.ps1 -MapperTasks 1
```

This test performs approximately one billion inner-loop operations. Use it for timing/scaling measurements, not for the normal quick correctness suite.
