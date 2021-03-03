module Distributor

import Distributed

"""
Launch workers locally and remotely. Note that you may have to activate your environment and load
the code on the launched workers explicity with `Distributed.@everywhere ...`.

`remote_nodes` must be an iterable of objects with properties
    -  `hostname`: the ssh host name of the node (e.g. as listed in your `~/.ssh/config`)
    -  and `n_workers`: an integer > 0 or `:auto` one worker per thread on the node.
"""
function start_workers(;
    remote_nodes,
    n_workers_local = 0,
    # NOTE: only the ssh tunnel is relevant for connecting to IPB remotely. `topology =
    # master_worker` seems neccessary to get ssh multi-plexing to work.
    cluster_config = (; tunnel = true, topology = :master_worker),
    sync_path = nothing,
)
    @assert all(remote_nodes) do n
        n.n_workers == :auto || n.n_workers > 0
    end

    if !isempty(remote_nodes)
        if !isnothing(sync_path)
            sync_remote_files(first(remote_nodes).hostname, sync_path)
        end

        Distributed.addprocs([(n.hostname, n.n_workers) for n in remote_nodes]; cluster_config...)
    end

    if n_workers_local == :auto || n_workers_local > 0
        Distributed.addprocs(n_workers_local)
    end

    Distributed.workers()
end

"Stop all workers."
function stop_workers()
    Distributed.rmprocs(Distributed.workers())
end

"Sync files via rsync to remote node. Assuming same path layout as on local machine."
function sync_remote_files(remote_node_hostname, path)
    run(`rsync -r -a -v -e ssh --delete $path/ $remote_node_hostname:$path`)
end

end # module
