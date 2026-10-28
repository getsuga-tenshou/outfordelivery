data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

resource "aws_security_group" "kafka" {
  name        = "${local.name}-kafka-${local.suffix}"
  description = "Kafka/Redpanda node"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH from you"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "Kafka external from you"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "Kafka internal from the VPC (Glue)"
    from_port   = 29092
    to_port     = 29092
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }
  ingress {
    description = "Schema Registry from you and the VPC"
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr, data.aws_vpc.default.cidr_block]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "kafka" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.kafka.id]
  key_name                    = var.ec2_key_name
  iam_instance_profile        = var.ec2_instance_profile
  associate_public_ip_address = true

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = <<-EOT
    #!/bin/bash
    set -euxo pipefail
    dnf update -y
    dnf install -y docker
    systemctl enable --now docker

    # Stop/start assigns a new public IP, so cloud-init re-runs this on every boot
    # to re-advertise the current address. Broker data lives in the redpanda-data volume.
    mkdir -p /var/lib/cloud/scripts/per-boot
    cat > /var/lib/cloud/scripts/per-boot/redpanda.sh <<'SCRIPT'
    #!/bin/bash
    set -euxo pipefail
    until docker info >/dev/null 2>&1; do sleep 2; done

    TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
    PRIVATE_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)
    PUBLIC_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)

    docker rm -f redpanda || true
    docker run -d --name redpanda --restart=unless-stopped \
      -v redpanda-data:/var/lib/redpanda/data \
      -p 9092:9092 -p 29092:29092 -p 8081:8081 -p 9644:9644 \
      docker.redpanda.com/redpandadata/redpanda:v24.1.7 \
      redpanda start --smp 1 --overprovisioned \
      --kafka-addr INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092 \
      --advertise-kafka-addr INTERNAL://$PRIVATE_IP:29092,EXTERNAL://$PUBLIC_IP:9092 \
      --schema-registry-addr 0.0.0.0:8081

    until docker exec redpanda rpk cluster health --exit-when-healthy >/dev/null 2>&1; do sleep 3; done
    docker exec redpanda rpk topic create parcel.events -p 6 -r 1 || true
    docker exec redpanda rpk topic create weather -p 1 -r 1 || true
    docker exec redpanda rpk topic create parcel.events.dlq -p 3 -r 1 || true
    SCRIPT
    chmod +x /var/lib/cloud/scripts/per-boot/redpanda.sh
    /var/lib/cloud/scripts/per-boot/redpanda.sh
  EOT

  tags = { Name = "${local.name}-kafka" }
}
