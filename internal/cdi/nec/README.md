# NEC CDI Plugin

This directory contains a prototype plugin for the composable-resource-operator
that targets **NEC CDI Solution**.

## Scope and Target Environment

NEC provides **CDI Manager (CDIM)** as an open-source project.
In addition, **NEC CDI Solution** is provided as a product that is based on CDIM.

The OSS version of CDIM currently supports only a simulator and does not provide
real controllable resources. For this reason, this plugin is currently implemented
to work **only with NEC CDI Solution**.

## Current Status

- This implementation provisionally supports **NEC CDI Solution only**
- It is not intended to work with the OSS-based CDIM at this stage

Details about this limitation are described here to avoid confusion.

## Future Direction

Although the current implementation is product-specific, the long-term goal is
to make this plugin commonly applicable to both the OSS-based CDIM and
NEC CDI Solution.

## Product Information

For details about NEC CDI Solution, please refer to the following page:  
<https://jpn.nec.com/cdi/index.html>