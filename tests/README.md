# Readme for the test dir layout

## Intro

In this codebase we are making use of *pytest* as testing framework. Whereby we use fixtures
for mocking and generating state.

The fixtures are (mostly) all defined in `conftest.py`. Making them available in all test files.

I tried to make a division between unit and integration testing. 
## unit tests

- using mocked services where possible (spark)
- mostly fast running

## integration tests- 
- mostly slow

