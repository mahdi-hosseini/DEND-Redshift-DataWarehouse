provider "aws" {
  region = "us-west-2"
}

data "aws_iam_policy_document" "redshift_assume_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_role" {
  name               = "redshift-role"
  assume_role_policy = "${data.aws_iam_policy_document.redshift_assume_policy.json}"
  path               = "/"
}

data "aws_iam_policy" "AmazonS3ReadOnlyAccess" {
  arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "redshift_role_policy_attach" {
  role       = "${aws_iam_role.redshift_role.name}"
  policy_arn = "${data.aws_iam_policy.AmazonS3ReadOnlyAccess.arn}"
}

resource "aws_default_vpc" "default" {}

resource "aws_security_group" "redshift_sg" {
  name        = "redshift-sg"
  description = "Allow inbound traffic to Redshift cluster"
  vpc_id      = "${aws_default_vpc.default.id}"

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "TCP"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port       = 0
    to_port         = 0
    protocol        = "TCP"
    cidr_blocks     = ["0.0.0.0/0"]
  }
}

resource "aws_redshift_cluster" "dend_cluster" {
  cluster_identifier = "redshift-cluster"
  database_name      = "dev"
  master_username    = "awsuser"
  master_password    = "${var.cluster_master_password}"
  node_type          = "dc2.large"
  cluster_type       = "multi-node"
  number_of_nodes    = 2
  skip_final_snapshot = true
  iam_roles          = ["${aws_iam_role.redshift_role.arn}"]
  vpc_security_group_ids = ["${aws_security_group.redshift_sg.id}"]
}

resource "aws_iam_user" "airflow_redshift_user" {
  name = "airflow_redshift_user"
}

data "aws_iam_policy" "AmazonRedshiftFullAccess" {
  arn = "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"
}

resource "aws_iam_user_policy_attachment" "airflow_redshift_role_policy_attach" {
  user       = "${aws_iam_user.airflow_redshift_user.name}"
  policy_arn = "${data.aws_iam_policy.AmazonRedshiftFullAccess.arn}"
}

resource "aws_s3_bucket" "dend_project_3" {
    bucket = "${var.dend_project_3}"
    acl = "private"
    force_destroy = true
}