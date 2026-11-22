<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# PLANTED

`inject.gotmpl` is inserted into a copy of the Helmfile. A template `exec`
can open a socket whatever the proxy variables say, so the render gate must
refuse it statically, before anything runs.
