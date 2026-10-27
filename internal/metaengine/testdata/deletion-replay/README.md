# Historical deletion replay fixtures

Captured on 2026-10-26 from detached source revision `8e57798`, before this
feature, by restoring the owning-package deletion fixtures and applying an
encoded BeginOperationProposal through Machine.Apply at the next committed
index. Required-stop uses deletionApprovalFixture; inherited-debt uses
existingDebtDeletionFixture; pending-readmission uses readmissionDeletionFixture.
Each proposal receives the correlation ID `historical-<case>`.

`before.json` is the original pre-deletion snapshot; `command.json` is the old
encoder output; `after.json` is the original FSM's snapshot after applying it.
The current tests must reproduce the complete original result byte-for-byte.
These are additional fixtures; existing API baseline artifacts were not modified.
Do not regenerate these using the current implementation.

SHA-256:

e511c9806135ddc6b3953114f3e5acc060ed12c7bc1f7d31ed5c04deeaf12266  inherited-debt/after.json
8d1ad0467a3e12e708bbbc60cdb27f127b1576925e63fc54710e6e19f79baa62  inherited-debt/before.json
e1f521b41fef754ff9f72f13609a3e0919f8da6ff761da2332f7527f4d415d7e  inherited-debt/command.json
77263886866fd510c4f02a1500bfac9b9545c20697bf49f7c8fd5e4adfddba6d  pending-readmission/after.json
dce1ea7aff187628a7fdfdd6723245780417b390679015f5c4459c720c21efc0  pending-readmission/before.json
bc915b0cff5e85fbc0b14768cfed90b2b559a0bfc7609161f867680f43c58629  pending-readmission/command.json
584996c6a5e24066674d26a532f71775c5364cd7886768f9383336b0a7c0eba3  required-stop/after.json
1eda44f78cc4c24fc9ac20bf1400fcafc812cd5c1ef9ea5fcbae19d4f877fc6d  required-stop/before.json
c980e18c343703a374ca2a221654c437ad98f3a53fd3a556e7adcf92b38d06af  required-stop/command.json
