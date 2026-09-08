+++
title = "Distributed processing"
description = "The same Pipeline runs on several nodes at once: each claims a row range under a lease, checkpoints Arrow IPC bytes through raft, and acks. Leases expire, so processing is at-least-once."
template = "distributed.html"
weight = 8
aliases = ["/distributed/"]
+++
