<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# Third-party components

<!-- yq: .charts[] | "| " + .name + " | " + .version + " |" -->

| Chart | Version |
| --- | --- |
| demo | 1.0.0 |

<!-- yq: .charts[] | .name as $p | .subcharts[] | "| " + $p + " | " + .name + " | " + (.version | tostring) + " |" -->

| Chart | Subchart | Version |
| --- | --- | --- |
| demo | sub | 0.1.0 |

<!-- yq: .images[] | select(.use == "used") | "| " + .ref + " | " + .use + " |" -->

| Image | Use |
| --- | --- |
| quay.io/example/tool:1.0.0@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | used |

<!-- yq: .built_images[] | "| " + .id + " |" -->

| Image |
| --- |
| demo-image |

End of the tables.
