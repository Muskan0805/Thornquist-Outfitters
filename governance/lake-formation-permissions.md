# Lake Formation Permission Set (design, not deployed)

Lab status: `aws lakeformation get-data-lake-settings` returns AccessDenied under the Learner Lab role. This permission set is documented as the governance design. The fallback is the IAM policy (analyst-least-privilege.json), the curated bucket policy (curated-bucket-policy.json), and the Athena workgroup restriction.

| Principal | Resource | Permissions | Status |
|---|---|---|---|
| analyst (hypothetical) | thornquist_trusted_david26.curated_all | SELECT, DESCRIBE | Designed, not deployed |
| analyst | landing, raw, trusted, quarantine tables | None | Designed, not deployed |
| LabRole | All databases | Full | Lab admin |

Rule: analyst sees curated only. Upstream zones and quarantine are not granted.
