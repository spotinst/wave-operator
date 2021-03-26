## Jupyter Enterprise Gateway Kernelspecs

### Docker image containing a set of kernelspecs for use with Wave

#### Build

To create and publish a new image:

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

To add custom kernelspecs, create a new docker image using this image as the base.
Copy the new kernelspec into the same _/kernels_ directory. After publishing the new
images, update the wavecomponent/enterprise-gateway _spec.valuesConfiguration_
field with the new custom image.
