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
cd "${REPO_ROOT}" || exit 1
GOPATH_BIN="$(go env GOPATH)/bin/"
export PATH="${GOPATH_BIN}:${PATH}"
RESOURCE_TYPE="${RESOURCE_TYPE:-"powervs-service"}"
NO_OF_RETRY=${NO_OF_RETRY:-"3"}

# shellcheck source=../ensure/ensure-go.sh
source "${REPO_ROOT}/hack/scripts/ensure/ensure-go.sh"
# shellcheck source=../ensure/ensure-kubectl.sh
source "${REPO_ROOT}/hack/scripts/ensure/ensure-kubectl.sh"
# shellcheck source=./boskos.sh
source "${REPO_ROOT}/hack/scripts/ci/boskos.sh"
# shellcheck source=../dev/kind-network-fix.sh
source "${REPO_ROOT}/hack/scripts/dev/kind-network-fix.sh"

ARTIFACTS="${ARTIFACTS:-${PWD}/_artifacts}"
mkdir -p "${ARTIFACTS}/logs/"

ARCH=$(uname -m)
OS=$(uname -s)
IBMCLOUD_CLI_VERSION=${IBMCLOUD_CLI_VERSION:-"2.47.0"}
POWER_PLUGIN_VERSION=${POWER_PLUGIN_VERSION:-"1.10.0"}
E2E_FLAVOR=${E2E_FLAVOR:-}
capibmadm=$(pwd)/bin/capibmadm

[ "${ARCH}" == "x86_64" ] && ARCH="amd64"

trap cleanup EXIT

cleanup(){
    local status=$?
    # stop the boskos heartbeat
    [[ -z ${HEART_BEAT_PID:-} ]] || kill -9 "${HEART_BEAT_PID}" || true
    report_lost_managed_lease "${status}" "${E2E_MANAGED_LEASE_LOST_FILE:-}"
}

retry() {
  cmd=$1
  for i in $(seq 1 "$NO_OF_RETRY"); do
    echo "Attempt: $i/$NO_OF_RETRY"
    ret_code=0
    $cmd || ret_code=$?
    if [ $ret_code = 0 ]; then
      break
    elif [ "$i" == "$NO_OF_RETRY" ]; then
      echo "All retry attempts failed!"
      exit $ret_code
    else
      sleep 1
    fi
  done
}

install_ibmcloud_cli(){
    if [ ${OS} == "Linux" ]; then
        platform="linux_${ARCH}"
    elif [ ${OS} == "Darwin" ]; then
        platform="macos"
    fi
     
    curl https://download.clis.cloud.ibm.com/ibm-cloud-cli/${IBMCLOUD_CLI_VERSION}/binaries/IBM_Cloud_CLI_${IBMCLOUD_CLI_VERSION}_${platform}.tgz -o IBM_Cloud_CLI_${IBMCLOUD_CLI_VERSION}_${platform}.tgz
    tar -xf IBM_Cloud_CLI_${IBMCLOUD_CLI_VERSION}_${platform}.tgz
    install IBM_Cloud_CLI/ibmcloud /usr/local/bin 

}

create_powervs_network_instance(){
    install_ibmcloud_cli

    ibmcloud config --check-version=false
    # Login to IBM Cloud using the API Key
    retry "ibmcloud login -a cloud.ibm.com --no-region"

    # Install power-iaas command-line plug-in and target the required service instance
    ibmcloud plugin install power-iaas -f -v ${POWER_PLUGIN_VERSION}
    CRN=$(ibmcloud resource service-instance ${IBMPOWERVS_WORKSPACE_ID} --output json | jq -r '.[].crn')
    ibmcloud pi workspace target ${CRN}

    # Create the network instance
    ${capibmadm} powervs network create --name ${IBMPOWERVS_NETWORK_NAME} --dns-servers 1.1.1.1,8.8.8.8,9.9.9.9 --service-instance-id ${IBMPOWERVS_WORKSPACE_ID} --zone ${IBMPOWERVS_ZONE}

}

init_network_powervs(){
    # Builds the capibmadm binary 
    make capibmadm

    create_powervs_network_instance

    # Creating PowerVS network port
    ${capibmadm} powervs port create --network ${IBMPOWERVS_NETWORK_NAME} --description "capi-e2e" --service-instance-id ${IBMPOWERVS_WORKSPACE_ID} --zone ${IBMPOWERVS_ZONE}

    # Get and assign the IPs to the required variables
    NEW_PORT=$(${capibmadm} powervs port list --service-instance-id ${IBMPOWERVS_WORKSPACE_ID} --zone ${IBMPOWERVS_ZONE} --network ${IBMPOWERVS_NETWORK_NAME} -o json)
    no_of_ports=$(echo ${NEW_PORT} | jq '.items | length')
    if [[ ${no_of_ports} != 1 ]]; then
        echo "Failed to get the required number or ports, got - ${no_of_ports}"
        exit 1
    fi
    export IBMPOWERVS_VIP="$(echo ${NEW_PORT} | jq -r '.items[0].ipAddress')"
    export IBMPOWERVS_VIP_EXTERNAL="$(echo ${NEW_PORT} | jq -r '.items[0].externalIP')"
    export IBMPOWERVS_VIP_CIDR=${IBMPOWERVS_VIP_CIDR:="29"}
}

prerequisites_powervs(){
    # Assigning PowerVS variables
    export IBMPOWERVS_SSHKEY_NAME=${IBMPOWERVS_SSHKEY_NAME:-"powercloud-bot-key"}
    export IBMPOWERVS_IMAGE_NAME=${IBMPOWERVS_IMAGE_NAME:-"capibm-powervs-centos-streams10-1-35-9"}
    export IBMPOWERVS_WORKSPACE_ID=${BOSKOS_SERVICE_INSTANCE_ID:-"d53da3bf-1f4a-42fa-9735-acf16b1a05cd"}
    export IBMPOWERVS_NETWORK_NAME="capi-net-$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | head --bytes 5)"
    export IBMPOWERVS_REGION=${BOSKOS_REGION:-"osa"}
    export IBMPOWERVS_ZONE=${BOSKOS_ZONE:-"osa21"}
    init_network_powervs
}

prerequisites_vpc(){
    # Assigning VPC variables
    export IBMVPC_REGION=${BOSKOS_REGION:-"jp-osa"}
    export IBMVPC_ZONE="${IBMVPC_REGION}-1"
    export IBMVPC_RESOURCEGROUP=${BOSKOS_RESOURCE_GROUP:-"fa5405a58226402f9a5818cb9b8a5a8a"}
    export IBMVPC_NAME=${BOSKOS_RESOURCE_NAME:-"capi-vpc-e2e"}
    export IBMVPC_IMAGE_NAME=${IBMVPC_IMAGE_NAME:-"capibm-vpc-ubuntu-2404-kube-v1-35-9"}
    export IBMVPC_PROFILE=${IBMVPC_PROFILE:-"bx2-4x16"}
    export IBMVPC_SSHKEY_NAME=${IBMVPC_SSHKEY_NAME:-"vpc-cloud-bot-key"}
}

# require_managed_variable fails the run before any cluster is created when a variable of the managed lanes is empty.
# $1 is the variable name, $2 tells the operator what to set.
require_managed_variable(){
    if [[ -z "${!1:-}" ]]; then
        echo "managed e2e (${E2E_FLAVOR}): ${1} is not set; ${2}" >&2
        exit 1
    fi
}

# validate_managed_flavor fails the run, before a Boskos lease is taken, unless E2E_FLAVOR names a managed lane. Any
# other managed-* flavor selects the managed e2e config and the managed specs (make set-flavor, GINKGO_FOCUS), but both
# lanes then skip themselves, so the job would pass without testing anything.
validate_managed_flavor(){
    case "${E2E_FLAVOR}" in
        managed-iks|managed-roks-single-zone) ;;
        *)
            echo "managed e2e: E2E_FLAVOR=${E2E_FLAVOR} runs no managed lane; set E2E_FLAVOR=managed-iks (IKS lane," \
                "3 zones x 2 workers) or E2E_FLAVOR=managed-roks-single-zone (ROKS lane, 1 zone x 2 workers)" >&2
            exit 1
            ;;
    esac
}

prerequisites_managed(){
    # Managed Kubernetes (IKS and ROKS) lanes: test/e2e/managed_test.go with test/e2e/config/ibmcloud-e2e-managed.yaml.
    # The e2e VPC is an existing VPC with one existing subnet in each of three zones of the region; CAPIBM never
    # creates or deletes VPC resources. Every value can come from the environment; Boskos supplies the region, the
    # resource group and the VPC name. The zones default to <region>-1..3 and the subnets to <vpc name>-<zone>.
    export IBMCLOUD_REGION=${IBMCLOUD_REGION:-${BOSKOS_REGION:-}}
    export IBMCLOUD_RESOURCE_GROUP_ID=${IBMCLOUD_RESOURCE_GROUP_ID:-${BOSKOS_RESOURCE_GROUP:-}}
    export IBMCLOUD_VPC_NAME=${IBMCLOUD_VPC_NAME:-${BOSKOS_RESOURCE_NAME:-}}
    require_managed_variable IBMCLOUD_API_KEY "export the IBM Cloud API key the controller runs with"
    require_managed_variable IBMCLOUD_REGION "export the region of the e2e VPC, for example eu-de"
    require_managed_variable IBMCLOUD_RESOURCE_GROUP_ID "export the ID of the e2e resource group"
    require_managed_variable IBMCLOUD_VPC_NAME "export the name of the e2e VPC"
    export IBMCLOUD_ZONE_1=${IBMCLOUD_ZONE_1:-"${IBMCLOUD_REGION}-1"}
    export IBMCLOUD_ZONE_2=${IBMCLOUD_ZONE_2:-"${IBMCLOUD_REGION}-2"}
    export IBMCLOUD_ZONE_3=${IBMCLOUD_ZONE_3:-"${IBMCLOUD_REGION}-3"}
    export IBMCLOUD_SUBNET_NAME_1=${IBMCLOUD_SUBNET_NAME_1:-"${IBMCLOUD_VPC_NAME}-${IBMCLOUD_ZONE_1}"}
    export IBMCLOUD_SUBNET_NAME_2=${IBMCLOUD_SUBNET_NAME_2:-"${IBMCLOUD_VPC_NAME}-${IBMCLOUD_ZONE_2}"}
    export IBMCLOUD_SUBNET_NAME_3=${IBMCLOUD_SUBNET_NAME_3:-"${IBMCLOUD_VPC_NAME}-${IBMCLOUD_ZONE_3}"}
    if [[ "${E2E_FLAVOR}" == "managed-roks-single-zone" ]]; then
        require_managed_variable COS_INSTANCE_CRN "export the CRN of the standard COS instance for the ROKS image registry"
    fi
    export EXP_MANAGED_KUBERNETES=true
    # Only the managed specs run. The lanes stop their own steps at their hard timeouts (4 h for each lane, counted
    # from the lane's set-up); Ginkgo's timeout adds 1 h 30 m for the management cluster set-up and the first
    # cleanups. It does not bound the cleanups: each runs within its own NodeTimeout (CAPI deletion 90 m, IBM backstop
    # 10 m, inventory diff 45 m), and Ginkgo v2 grants a cleanup that starts after the suite timeout its full
    # NodeTimeout, so the IBM backstop always runs and a run can last up to GINKGO_TIMEOUT plus 145 m. The Boskos
    # lease is held for all of it (heartbeat_managed_lease).
    export GINKGO_FOCUS="Managed Kubernetes"
    export GINKGO_TIMEOUT=${GINKGO_TIMEOUT:-"5h30m"}
}

main(){

    [[ "${E2E_FLAVOR}" == "vpc" ]] && RESOURCE_TYPE="vpc-service"
    # The managed lanes lease a VPC with three subnets. The type name contains "vpc", so boskos.sh exports the resource
    # group of the lease. With one resource of this type per e2e account, and the lease held until the run returns
    # (heartbeat_managed_lease), only one managed run at a time uses an e2e account.
    if [[ "${E2E_FLAVOR}" == managed-* ]]; then
        validate_managed_flavor
        RESOURCE_TYPE=${MANAGED_RESOURCE_TYPE:-"managed-kubernetes-vpc-service"}
    fi

    # If BOSKOS_HOST is set then acquire an IBM Cloud resource from Boskos.
    if [ -n "${BOSKOS_HOST:-}" ]; then
        # Check out the resource from Boskos and store the produced environment
        # variables in a temporary file.
         account_env_var_file="$(mktemp)"
         checkout_account "${RESOURCE_TYPE}" 1> "${account_env_var_file}"
         checkout_account_status="${?}"

        # If the checkout process was a success then load the
        # environment variables into this process.
        [ "${checkout_account_status}" = "0" ] && . "${account_env_var_file}"

        # Always remove the account environment variable file which
        # could contain sensitive information.
        rm -f "${account_env_var_file}"

        if [ ! "${checkout_account_status}" = "0" ]; then
            echo "error getting account from boskos" 1>&2
            exit "${checkout_account_status}"
        fi

        # Run the heart beat process to tell Boskos that we are still
        # using the checked out resource periodically.
        if [[ "${E2E_FLAVOR}" == managed-* ]]; then
            # The heartbeat reports a refused renewal through this file: the lanes stop their steps once it appears
            # (test/e2e/managedtools/lease.go), and cleanup fails the job with it (report_lost_managed_lease).
            export E2E_MANAGED_LEASE_LOST_FILE="${ARTIFACTS}/logs/boskos-lease-lost"
            rm -f "${E2E_MANAGED_LEASE_LOST_FILE}"
            heartbeat_managed_lease "${E2E_MANAGED_LEASE_LOST_FILE}" >> "$ARTIFACTS/logs/boskos.log" 2>&1 &
        else
            heartbeat_account >> "$ARTIFACTS/logs/boskos.log" 2>&1 &
        fi
        HEART_BEAT_PID=$(echo $!)
    fi

    # Set common variables
    export DOCKER_BUILDKIT=1
    export IBMACCOUNT_ID=${IBMACCOUNT_ID:-"7cfbd5381a434af7a09289e795840d4e"}
    export BASE64_API_KEY=$(tr -d '\n' <<<"$IBMCLOUD_API_KEY" | base64)
    export CUSTOM_KIND_NODE_IMAGE=${CUSTOM_KIND_NODE_IMAGE:-}
    # Setting controller loglevel to allow debug logs from the VPC/PowerVS client
    export LOGLEVEL=5

    if [[ "${E2E_FLAVOR}" == "powervs" || "${E2E_FLAVOR}" == "powervs-md-remediation" ]]; then
        prerequisites_powervs
    fi

    if [[ "${E2E_FLAVOR}" == "vpc" ]]; then
        prerequisites_vpc
    fi

    if [[ "${E2E_FLAVOR}" == managed-* ]]; then
        prerequisites_managed
    fi

    # Run the e2e tests
    make test-e2e E2E_FLAVOR=${E2E_FLAVOR}
    test_status="${?}"
    echo TESTSTATUS="${test_status}"

    # If Boskos is being used then release the IBM Cloud resource back to Boskos.
    [ -z "${BOSKOS_HOST:-}" ] || release_account >> "$ARTIFACTS/logs/boskos.log" 2>&1
}

if [[ $- =~ x ]]; then
    set +x
fi

main "$@"
exit "${test_status}"
