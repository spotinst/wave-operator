## Jupyter Enterprise Gateway Kernelspecs

### Docker image containing a set of kernelspecs for use with Wave

#### Build

To create a publish a new image

```shell
make docker-build
make docker-push
```

#### Use

The enterprise-gateway helm chart optionally specifies a kernelspec image to use as an 
init-container. On startup, the contents of the _/kernels_ directory are copied to 
_/usr/local/share/jupyter/kernels_ and made available as base notebook kernels.

This image is specified in the helm chart as the value _kernelspecs.image_

#### Extend

This image can be used as the base image for pther kernels by simply copying more 
kernelspecs into the directory then and updating the wavecomponent spec.valuesConfiguration 
field with the new image.
