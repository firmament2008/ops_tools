#!/bin/bash
#redis cluster slot controller
# SETSLOTS 构建slot
# ADDNODE  添加节点
# REMOVENODE 删除节点
# MERGESLOTS 迁移slot
# info 集群查询
# node 节点查询
# REPLICATE 从节点调整

COMMAMD=$2
PORT=${1:-6380}
PID="$$"x

function cleantemp(){
    rm -rf /tmp/${PID}
}

function setmap(){
    mkdir -p /tmp/${PID}
    nodes \
    | grep ${PORT} \
    | awk ' {split($2,x,"@") ;\
             split(x[1],y,":")  ;\
             print $1 , y[1],y[2]}
           '\
    | while read line
      do
        echo   ${line#* }> /tmp/${PID}/${line%% *}
      done

    }


function specConn1(){
    [ -d /tmp/${PID} ] ||  setmap
    _nodeid=$1
    _nodeinfo=$( cat /tmp/${PID}/${_nodeid} )
    _ip=${_nodeinfo% *}
    _port=${_nodeinfo#* }
    sCMD1="/usr/local/bin/redis-cli -c -h ${_ip} -p ${_port}"
}

function specConn2(){
    [ -d /tmp/${PID} ] ||  setmap
    _nodeid=$1
    _nodeinfo=$( cat /tmp/${PID}/${_nodeid} )
    _ip=${_nodeinfo% *}
    _port=${_nodeinfo#* }
    sCMD2="/usr/local/bin/redis-cli -c -h ${_ip} -p ${_port}"
}


function connect(){
    _port=$1
    CMD="/usr/local/bin/redis-cli -c -p ${_port}"
}

function info(){
    ${CMD} cluster info
}

function nodes(){
    ${CMD} cluster nodes
}

function cleanErrNode(){

    #echo "usage: host port"
    nodes_addrs=$( nodes |grep -v handshake| awk '{print $2}' )
    echo $nodes_addrs
    for addr in ${nodes_addrs[@]}; do
        host=${addr%:*}
        port=${addr#*:}
        del_nodeids=$( timeout 5 /usr/local/bin/redis-cli -h $host -p $port cluster nodes|grep -E 'handshake|fail'| awk '{print $1}' )
        for nodeid in ${del_nodeids[@]}; do
            echo $host $port $nodeid
            /usr/local/bin/redis-cli -h $host -p $port cluster forget $nodeid
        done
    done
}


function REMOVENODE(){
    _nodeid=$1
    for node in `nodes \
                 | grep -v ${_nodeid} \
                 | awk '{print $1}' `
    do
        specConn1 ${node}
        timeout 5 ${sCMD1} cluster  forget ${_nodeid}
    done

}

function ADDNODE(){
    _ip=$1
    _port=$2
    ${CMD} cluster meet ${_ip} ${_port}
}

function SETSLOTS(){
    start=$1
    end=$2
    nodeID=$3

    for slot in `seq ${start} ${end}`
    do
        echo "slot:${slot}"
        ${CMD} cluster setslot ${slot} node ${nodeID}
    done
}

function countslotkeys(){
    slot="$1"
    _c=$(${CMD} cluster getkeysinslot ${slot} 100  | wc -c )
    if [ ${_c} -gt 1 ];then
        ${CMD} cluster getkeysinslot ${slot} 100
    else
        echo "$$x"
    fi

}
gkey=""
function MERGEKEYS(){
    sCMD1="$1"
    slot="$2"
    _nodeid="$3"

    _nodeinfo=$( cat /tmp/${PID}/${_nodeid} )
    _ip=${_nodeinfo% *}
    _port=${_nodeinfo#* }

    keys=$( countslotkeys ${slot} )
    if [[ $(${CMD} cluster getkeysinslot ${slot} 100  | wc -c ) -gt 1 ]] ;then
        for key in ${keys[@]};do
            #MIGRATE host port key| destination-db timeout [COPY] [REPLACE] [KEYS key]
            if ${key} == ${gkey} ;then
                ${sCMD1} MIGRATE ${_ip} ${_port} ${key} 0 1000
                gkey="${key}"
            else
                ${sCMD1} MIGRATE ${_ip} ${_port} ${key} 0 1000 replace
            fi
        done
    fi
}

function MERGESLOTS(){
    # 集群数据迁移
    # 在手动进行数据迁移时，需要执行以下步骤：
    # 1.在源节点和目标节点分别使用CLUSTER SETSLOT MIGRATING和CLUSTER SETSLOT IMPORTING标记slot迁出和迁入信息
    # 2.在源节点使用CLUSTER GETKEYSINSLOT 命令获取待迁出的KEY
    # 3.在源节点执行MIGRATE命令进行数据迁移，MIGRATE既支持单个KEY的迁移，也支持多个KEY的迁移
    # 4.在源节点和目标节点使用CLUSTER SETSLOT命令标记slot最终迁移节点

    start=$1
    end=$2
    fromNode=$3
    toNode=$4
    for slot in `seq ${start} ${end}`
    do
        echo "slot:${slot}"

        specConn1 ${fromNode}
        specConn2 ${toNode}

        ${sCMD2} cluster setslot ${slot} importing ${toNode} \
            && ( \
                ${sCMD1} cluster setslot ${slot} mirgrating ${fromNode} \
                || ${sCMD2} cluster setslot ${slot} stable  \
            ) \
            || ${sCMD2} cluster setslot ${slot} stable


        # ${sCMD1} CLUSTER  GETKEYSINSLOT ${slot} 100
        while [[ $(countslotkeys $slot) != $$x ]]; do
            MERGEKEYS "${sCMD1}" "${slot}" "${toNode}"
            #statements
        done


        ${sCMD1} cluster setslot ${slot} node ${toNode} ;
        ${sCMD2} cluster setslot ${slot} node ${toNode} ;


        #${sCMD1} cluster setslot ${slot} stable


    done
}

function REPLICATE(){
    _slave=$1
    _master=$2
    specConn1 ${_slave}
    ${sCMD1} cluster REPLICATE ${_master}
}

connect ${PORT}

case ${COMMAMD} in
    info)
        info
    ;;
    nodes)
        nodes
    ;;
    REMOVENODE)
        NODEID=$3
        cleanErrNode
        REMOVENODE ${NODEID}

    ;;
    SETSLOTS)
        start=$3
        end=$4
        nodeID=$5
        SETSLOTS ${start} ${end} ${nodeID}
    ;;
    MERGESLOTS)
        start=$3
        end=$4
        fromNodeID=$5
        toNodeID=$6
        MERGESLOTS ${start} ${end} ${fromNodeID} ${toNodeID}
    ;;
    ADDNODE)
        ip=$3
        port=$4
        ADDNODE ${ip} ${port}
    ;;
    REPLICATE)
        slave=$3
        master=$4
        REPLICATE ${slave} ${master}
    ;;
    *)
    echo @! $@
    info
    echo "###slave###"
    nodes | grep slave
    echo "###master###"
    nodes  | grep master
    echo "######"

    echo "帮助信息"
    echo """
# 构建slot: SETSLOTS start end nodeID
    eg: $0 ${PORT} SETSLOTS 0 5461 f0b281d833d357fe5137805afad18e5355c5e7b7
        @! ${PORT} SETSLOTS 5462 10922 2f3391d3982bb8f385ba4a64da073e4931b5a72b
        @! ${PORT} SETSLOTS 10923 16383 620f68d978402da92f368797ea743f07f76a7961
# 添加节点: ADDNODE ip port
    eg: $0 ${PORT} ADDNODE a.b.c.d port
# 删除节点: REMOVENODE NODEID
    eg: $0 ${PORT} REMOVENODE 31c5368da4827d33082a44f95ff88abb92e95078
# 迁移slot: MERGESLOTS start end fromNode toNode
    eg: $0 ${PORT} MERGESLOTS 1 3 2f3391d3982bb8f385ba4a64da073e4931b5a72b 620f68d978402da92f368797ea743f07f76a7961
# 从节点调整: REPLICATE slave master
    eg: $0 ${PORT}  REPLICATE 620f68d978402da92f368797ea743f07f76a7961 1d4e803b8218b83a5f426c3cc913cbf38f22ac85
# 集群查询: info
    eg: $0 ${PORT} info
# 节点查询: node
    eg: $0 ${PORT} node

    """
    ;;

esac

cleantemp