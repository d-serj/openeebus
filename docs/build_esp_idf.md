# Building with ESP-IDF

The repository root can be consumed directly as an ESP-IDF component.

## Add the component to your project

In your ESP-IDF project's `idf_component.yml`:

```yaml
dependencies:
  openeebus:
    git: https://github.com/NIBEGroup/openeebus.git
```

## Additional dependency

This component depends on a `libwebsockets` ESP-IDF component. Your project must provide that dependency as well, either from your own component tree or through an additional entry in `idf_component.yml`.

## Notes

* The ESP-IDF component build enables the repository's FreeRTOS-specific implementations automatically.
* The standalone desktop CMake build remains unchanged.
