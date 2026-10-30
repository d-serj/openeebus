# Tests

The test suites are grouped by type:

```text
tests/
├── CMakeLists.txt
├── unit_tests/
│   ├── CMakeLists.txt
│   ├── common/
│   ├── service/
│   ├── ship/
│   ├── spine/
│   ├── use_case/
│   └── mocks/
└── integration_tests/
    ├── CMakeLists.txt
    ├── conftest.py
    ├── requirements.txt
    └── test_*.py
```

`unit_tests/` contains the GoogleTest/GoogleMock tests, grouped by production module.
Shared helpers are directly under `unit_tests/`; module-specific fixtures and test data
are beside their tests. Test doubles are under `unit_tests/mocks/`.

`integration_tests/` contains pytest scenarios that run the demo applications as separate
processes.

## Unit tests

From the repository root, configure and run the standalone unit suite:

```sh
cmake -S tests -B tests/build -G Ninja
cmake --build tests/build
ctest --test-dir tests/build --output-on-failure
```

To run a focused test target:

```sh
ctest --test-dir tests/build -R '^string_util_test$' --output-on-failure
```

Unit tests are also included in the root build when `OPTION_OPENEEBUS_TESTS=ON`
(the default).

## Unit and integration tests

Build the library, demo applications, and unit tests together, with integration
tests registered in CTest:

```sh
cmake -S . -B build -G Ninja -DOPTION_OPENEEBUS_TESTS=ON -DOPTION_INTEGRATION_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

To run only integration tests:

```sh
ctest --test-dir build -L integration --output-on-failure
```

See [Integration Tests](../docs/integration_tests.md) for dependencies, configuration,
and direct pytest commands. Generated binaries and reports belong in the build
directories.
