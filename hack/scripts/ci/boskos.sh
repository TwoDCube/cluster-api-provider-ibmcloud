#!/bin/bash

# Copyright 2022 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

REPO_ROOT=$(dirname "${BASH_SOURCE[0]}")/../../..

USER=${USER:-"cluster-api-provider-ibmcloud"}

release_account(){
    url="http://${BOSKOS_HOST}/release?name=${BOSKOS_RESOURCE_NAME}&dest=dirty&owner=${USER}"
    status_code=$(curl -w '%{http_code}' -X POST ${url})

    if [[ ${status_code} != 200 ]]; then
        echo "Got invalid response- ${status_code}"
        exit 1
    fi 
}

checkout_account(){
    if [[ $- =~ x ]]; then
        set +x
    fi
    resource_type=$1
    url="http://${BOSKOS_HOST}/acquire?type=${resource_type}&state=free&dest=busy&owner=${USER}"
    curl -X POST ${url} -o output.json
    [ $? = 0 ] && status_code=200

    if [[ ${status_code} == 200 ]]; then
        export BOSKOS_RESOURCE_NAME=$(jq -r '.name' < output.json)
        export BOSKOS_REGION=$(jq -r '.userdata["region"]' < output.json)
        api_key=$(jq -er '.userdata["api-key"]' < output.json 2>/dev/null) && export IBMCLOUD_API_KEY="$api_key"
        if [[  ${resource_type} =~ "powervs" ]]; then
            export BOSKOS_SERVICE_INSTANCE_ID=$(jq -r '.userdata["service-instance-id"]' < output.json)
            export BOSKOS_ZONE=$(jq -r '.userdata["zone"]' < output.json)
        elif [[  ${resource_type} =~ "vpc" ]]; then
            export BOSKOS_RESOURCE_GROUP=$(jq -r '.userdata["resource-group"]' < output.json)
        fi
        rm -rf output.json
    else
        echo "Got invalid response- ${status_code}"
        exit 1
    fi
}

heartbeat_account(){
    count=0
    url="http://${BOSKOS_HOST}/update?name=${BOSKOS_RESOURCE_NAME}&state=busy&owner=${USER}"
    while [ ${count} -lt 120 ]
    do
        status_code=$(curl -s -o /dev/null -w '%{http_code}' -X POST ${url})
        if [[ ${status_code} != 200 ]]; then
            echo "Got invalid response - ${status_code}"
            exit 1
        fi
        count=$(( $count + 1 ))
        sleep 60
    done
}

# heartbeat_managed_lease keeps the Boskos lease of a managed Kubernetes e2e run alive until its caller kills it
# (ci-e2e.sh does so in its EXIT trap). heartbeat_account above suits the short runs: it stops after 120 updates and on
# the first failed one. A managed run holds IBM clusters for up to GINKGO_TIMEOUT plus the lanes' cleanups, and the
# janitor and the inventory diff of the next managed run rely on one run at a time per e2e account
# (test/e2e/managedtools/janitor.go JanitorMinimumAge), so a lease that the Boskos reaper reclaims while this run still
# owns clusters lets two runs share the account. This loop therefore never gives up on an error that may pass: a
# connection error, a request that runs into --max-time, and a 5xx, 408 or 429 answer are logged and retried after 5 s,
# doubling up to the 60 s cadence, so an outage of Boskos costs the lease only if it outlasts the reaper's expiry. Any
# other 4xx answer ends it: Boskos then refuses the renewal itself (the lease was reclaimed or belongs to another
# owner), and no retry can win the lease back. The loop then writes Boskos's answer to the file $1, through a temporary
# file so that a reader never sees it half written; the managed lanes check that file before and during every step
# (test/e2e/managedtools/lease.go) and stop, and report_lost_managed_lease fails the job with it.
# $1 is the file to write when Boskos refuses the renewal; its directory must exist.
heartbeat_managed_lease(){
    local lost_file="${1:?heartbeat_managed_lease needs the file to write when Boskos refuses the renewal}"
    local url="http://${BOSKOS_HOST}/update?name=${BOSKOS_RESOURCE_NAME}&state=busy&owner=${USER}"
    local cadence=60 first_retry=5 retry=5 status_code
    while true; do
        # curl prints 000 when it gets no answer (connection error, --max-time) and exits non-zero; "|| true" keeps
        # errexit, which the background shell inherits, from ending the loop there.
        status_code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 -X POST "${url}") || true
        status_code=${status_code:-000}
        if [[ "${status_code}" == 200 ]]; then
            retry=${first_retry}
            sleep "${cadence}"
            continue
        fi
        if [[ "${status_code}" =~ ^4[0-9][0-9]$ && "${status_code}" != 408 && "${status_code}" != 429 ]]; then
            echo "$(date -u +%FT%TZ) Boskos refused to renew the lease of ${BOSKOS_RESOURCE_NAME} with HTTP" \
                "${status_code}; the lease is lost and the account may already serve another run"
            printf 'Boskos answered HTTP %s to the renewal of %s at %s\n' "${status_code}" "${BOSKOS_RESOURCE_NAME}" \
                "$(date -u +%FT%TZ)" > "${lost_file}.tmp"
            mv -f "${lost_file}.tmp" "${lost_file}"
            return 1
        fi
        echo "$(date -u +%FT%TZ) renewing the lease of ${BOSKOS_RESOURCE_NAME} failed (HTTP ${status_code}); retrying" \
            "in ${retry} s"
        sleep "${retry}"
        retry=$(( retry * 2 < cadence ? retry * 2 : cadence ))
    done
}

# report_lost_managed_lease fails the job when heartbeat_managed_lease has reported that Boskos refused to renew the
# lease of a managed run. The account may then have served another managed run at the same time, whose janitor and
# inventory diff count on having it alone, so neither this run's results nor its leftover check can be trusted, even
# when every spec passed. ci-e2e.sh calls it from its EXIT trap, where its exit sets the job's exit status.
# $1 is the exit status the job would end with otherwise; a failed job keeps its own.
# $2 is the file heartbeat_managed_lease writes when Boskos refuses the renewal; empty when the run holds no lease.
report_lost_managed_lease(){
    local status="${1:?report_lost_managed_lease needs the exit status the job would end with}" lost_file="${2:-}"
    [[ -n "${lost_file}" && -f "${lost_file}" ]] || return 0
    echo "managed e2e: the Boskos lease of ${BOSKOS_RESOURCE_NAME:-the e2e account} was lost during the run:" \
        "$(cat "${lost_file}"). The lanes stopped their steps and cleaned up; before trusting this run, check the" \
        "account for clusters of another run, and the Boskos heartbeat log for the refused renewal." >&2
    [[ "${status}" != 0 ]] || exit 1
}
