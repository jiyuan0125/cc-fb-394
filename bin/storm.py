#!/usr/bin/env python3
"""Detect per-service ERROR storms in a mixed-service application log.

Log line format (space separated, one event per line):
    <time> <service> <level> <duration_ms> <summary>

Usage:
    python3 -I storm.py <log_path> <storm_out_path> <summary_out_path>

Detection criteria (also written verbatim at the top of both output files):

1. Grouping: lines are grouped by service; each service is analyzed
   independently. Lines are sorted by the timestamp printed on the line
   (the originating machine's own clock). No cross-machine clock
   correction is attempted; ties keep the original file order.
2. Window / trigger: a 60-second sliding window anchored at every ERROR
   line of the service (window = [t-60s, t] in that service's timestamp
   space). A window containing >= 5 ERROR lines marks every ERROR inside
   it as a "core" error. The first time a core error appears, a storm
   starts.
3. End / gap tolerance: while a storm is active, any two consecutive
   ERROR lines of the service more than 30 seconds apart split the run.
   The 30 s tolerance absorbs sporadic edge errors and short lulls; the
   observed data separates cleanly there (intra-storm gaps <= 30 s,
   inter-storm gaps >= 32 s).
4. Boundaries: a storm spans from the first to the last ERROR line of
   its cluster. Scattered non-core ERROR lines within 30 s of a cluster
   edge are pulled into the storm.
5. Counts: error_lines counts ERROR lines with timestamps in
   [start, end]; total_lines counts all lines of that service in the
   same interval (INFO/WARN/ERROR).
6. Duration is end_time - start_time in whole seconds (inclusive line
   timestamps, subtractive duration).
7. Services whose ERROR rate never reaches the trigger (no 60 s window
   with >= 5 ERRORs) are reported in the summary with 0 storms and a
   longest duration of "-".
The algorithm uses only the input lines and fixed parameters, so
repeated runs on the same file produce byte-identical output.
"""

import sys
from collections import defaultdict
from datetime import datetime

WINDOW_SECONDS = 60      # sliding window width
ERROR_THRESHOLD = 5      # >= this many ERRORs in a window => storm core
GAP_SECONDS = 30         # consecutive-ERROR gap that ends a storm

CRITERIA = """\
# ERROR storm detection criteria
# 1. Lines grouped by service; analyzed independently, sorted by the
#    timestamp printed on each line (local machine clock, no clock
#    correction); timestamp ties keep original file order.
# 2. Trigger: 60s sliding window [t-60s, t] anchored at every ERROR line;
#    >=5 ERROR lines in any window marks all ERRORs inside it as core
#    errors; first core error starts a storm.
# 3. End: consecutive ERROR lines of the service more than 30s apart
#    split the run (30s gap tolerance for edge/lull errors).
# 4. Storm interval = first..last ERROR of the cluster, including
#    non-core ERRORs within 30s of a cluster edge.
# 5. error_lines = ERROR lines in [start,end]; total_lines = all lines
#    (INFO/WARN/ERROR) of that service in [start,end].
# 6. Duration = end_time - start_time, whole seconds.
# 7. Services with no triggered storm appear in the summary with
#    0 storms and longest duration "-".
"""


def parse_time(value):
    return int(datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").timestamp())


def fmt_time(epoch):
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%dT%H:%M:%S")


def main():
    if len(sys.argv) != 4:
        sys.exit("usage: python3 -I storm.py <log> <storm_out> <summary_out>")
    log_path, storm_out_path, summary_out_path = sys.argv[1:4]

    # Per service: all lines as (epoch, order, level), errors separately.
    lines_by_service = defaultdict(list)
    errors_by_service = defaultdict(list)
    order = 0
    malformed = 0
    with open(log_path, "r", encoding="utf-8") as fh:
        for raw in fh:
            parts = raw.split(maxsplit=4)
            if len(parts) < 5:
                malformed += 1
                continue
            time_s, service, level = parts[0], parts[1], parts[2]
            try:
                epoch = parse_time(time_s)
            except ValueError:
                malformed += 1
                continue
            lines_by_service[service].append((epoch, order, level))
            if level == "ERROR":
                errors_by_service[service].append((epoch, order))
            order += 1

    if malformed:
        print(f"warning: skipped {malformed} malformed line(s)", file=sys.stderr)

    storm_rows = []       # (service, start_epoch, end_epoch, errors, total)
    summary = {}

    for service in sorted(lines_by_service):
        all_lines = sorted(lines_by_service[service])
        errors = sorted(errors_by_service.get(service, []))
        n_err = len(errors)

        # --- mark core errors: any 60s window [t-60, t] with >=5 ERRORs ---
        is_core = [False] * n_err
        left = 0
        for right in range(n_err):
            while errors[right][0] - errors[left][0] > WINDOW_SECONDS:
                left += 1
            if right - left + 1 >= ERROR_THRESHOLD:
                for k in range(left, right + 1):
                    is_core[k] = True

        # --- cluster core errors, splitting on >30s consecutive ERROR gaps ---
        clusters = []
        current = None
        prev_idx = None
        for i in range(n_err):
            if not is_core[i]:
                continue
            if current is None:
                current = [i, i]
                clusters.append(current)
            elif errors[i][0] - errors[prev_idx][0] > GAP_SECONDS:
                current = [i, i]
                clusters.append(current)
            else:
                current[1] = i
            prev_idx = i

        # --- extend each cluster over non-core errors within the gap
        #     tolerance, then merge clusters that overlap after extension ---
        spans = []
        for lo, hi in clusters:
            while lo > 0 and errors[lo][0] - errors[lo - 1][0] <= GAP_SECONDS:
                lo -= 1
            while hi + 1 < n_err and errors[hi + 1][0] - errors[hi][0] <= GAP_SECONDS:
                hi += 1
            if spans and lo <= spans[-1][1]:
                spans[-1][1] = max(spans[-1][1], hi)
            else:
                spans.append([lo, hi])

        service_storms = []
        for lo, hi in spans:
            start = errors[lo][0]
            end = errors[hi][0]
            err_count = sum(1 for t, _ in errors if start <= t <= end)
            total_count = sum(1 for t, _, _ in all_lines if start <= t <= end)
            service_storms.append((service, start, end, err_count, total_count))

        storm_rows.extend(service_storms)
        if service_storms:
            longest = max(end - start for _, start, end, _, _ in service_storms)
        else:
            longest = None
        summary[service] = (len(service_storms), longest)

    # deterministic ordering: service then start time (file order is the tiebreak)
    storm_rows.sort(key=lambda r: (r[0], r[1], r[2]))

    with open(storm_out_path, "w", encoding="utf-8") as fh:
        fh.write(CRITERIA)
        fh.write("#\n")
        fh.write("service\tstart_time\tend_time\tduration_s\terror_lines\ttotal_lines\n")
        for service, start, end, err_count, total_count in storm_rows:
            fh.write(
                f"{service}\t{fmt_time(start)}\t{fmt_time(end)}\t"
                f"{end - start}\t{err_count}\t{total_count}\n"
            )

    with open(summary_out_path, "w", encoding="utf-8") as fh:
        fh.write(CRITERIA)
        fh.write("#\n")
        fh.write("service\tstorm_count\tlongest_duration_s\n")
        for service in sorted(summary):
            count, longest = summary[service]
            fh.write(f"{service}\t{count}\t{'-' if longest is None else longest}\n")

    # console summary
    total_storms = sum(c for c, _ in summary.values())
    print(f"services={len(summary)} storms={total_storms}")
    for service in sorted(summary):
        count, longest = summary[service]
        dur = "-" if longest is None else f"{longest}s"
        print(f"  {service}: storms={count} longest={dur}")
    print(f"storm detail -> {storm_out_path}")
    print(f"summary      -> {summary_out_path}")


if __name__ == "__main__":
    main()
