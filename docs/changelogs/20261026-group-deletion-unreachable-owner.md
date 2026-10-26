# Group deletion with unavailable hosts

Newly approved group deletions complete with `_meta` quorum, even when former
hosts or the group's own quorum are unavailable. Returning hosts automatically
stop and clean up the deleted group. Physical cleanup progress remains separately
observable; public APIs are unchanged.
