output "redshift_cluster_endpoint" {
  description = "The connection endpoint"
  value       = "${aws_redshift_cluster.dend_cluster.endpoint}"
}

output "redshift_role_arn" {
  description = "The ARN of Redshift IAM role"
  value       = "${aws_iam_role.redshift_role.arn}"
}

output "s3_bucket_name" {
  description = "The name of Redshift bucket"
  value       = "${aws_s3_bucket.dend_project_3.bucket}"
}