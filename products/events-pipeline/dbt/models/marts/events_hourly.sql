-- SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
-- SPDX-License-Identifier: Apache-2.0
-- Hourly activity from the raw stream's table, next to that day's total from the
-- daily batch's table: a run after either one changes is a fresh mart.
with hourly as (
    select
        date_trunc('hour', event_time) as event_hour,
        cast(event_time as date) as event_date,
        count(*) as events,
        count(distinct user_id) as users
    from {{ source('events_raw', 'events') }}
    group by 1, 2
)
select
    hourly.event_hour,
    hourly.events,
    hourly.users,
    daily.events as day_events
from hourly
left join {{ source('events_curated', 'events_daily') }} as daily
    on daily.event_date = hourly.event_date
