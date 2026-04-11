/*
  Warnings:

  - A unique constraint covering the columns `[x25519_public_key]` on the table `User` will be added. If there are existing duplicate values, this will fail.

*/
-- AlterTable
ALTER TABLE "User" ADD COLUMN     "verfiication_id" TEXT,
ADD COLUMN     "x25519_public_key" TEXT;

-- CreateTable
CREATE TABLE "DirectMessage" (
    "id" TEXT NOT NULL,
    "sender_public_key" TEXT NOT NULL,
    "sender_x25519_public_key" TEXT NOT NULL,
    "recipient_public_key" TEXT NOT NULL,
    "message" TEXT NOT NULL,
    "message_hash" TEXT NOT NULL,
    "sender_signature" TEXT NOT NULL,
    "send_time" TIMESTAMP(3) NOT NULL,
    "algorithm" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "expires_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DirectMessage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ServerNode" (
    "peer_id" TEXT NOT NULL,
    "server_public_key" TEXT,
    "listen_addresses" JSONB,
    "last_seen_at" TIMESTAMP(3) NOT NULL,
    "last_announce_at" TIMESTAMP(3),
    "is_active" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ServerNode_pkey" PRIMARY KEY ("peer_id")
);

-- CreateTable
CREATE TABLE "ServerScoreReport" (
    "id" TEXT NOT NULL,
    "reporter_peer_id" TEXT NOT NULL,
    "reporter_server_public_key" TEXT NOT NULL,
    "target_peer_id" TEXT NOT NULL,
    "score" INTEGER NOT NULL,
    "observed_at" TIMESTAMP(3) NOT NULL,
    "expires_at" TIMESTAMP(3) NOT NULL,
    "payload_hash" TEXT NOT NULL,
    "signature" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ServerScoreReport_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ServerScoreAggregate" (
    "target_peer_id" TEXT NOT NULL,
    "mean_score" DOUBLE PRECISION NOT NULL,
    "report_count" INTEGER NOT NULL,
    "updated_at" TIMESTAMP(3) NOT NULL,
    "last_report_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ServerScoreAggregate_pkey" PRIMARY KEY ("target_peer_id")
);

-- CreateIndex
CREATE INDEX "DirectMessage_recipient_public_key_created_at_idx" ON "DirectMessage"("recipient_public_key", "created_at");

-- CreateIndex
CREATE INDEX "DirectMessage_expires_at_idx" ON "DirectMessage"("expires_at");

-- CreateIndex
CREATE INDEX "ServerNode_is_active_last_seen_at_idx" ON "ServerNode"("is_active", "last_seen_at");

-- CreateIndex
CREATE INDEX "ServerScoreReport_target_peer_id_expires_at_idx" ON "ServerScoreReport"("target_peer_id", "expires_at");

-- CreateIndex
CREATE INDEX "ServerScoreReport_reporter_peer_id_target_peer_id_observed__idx" ON "ServerScoreReport"("reporter_peer_id", "target_peer_id", "observed_at");

-- CreateIndex
CREATE UNIQUE INDEX "ServerScoreReport_reporter_peer_id_target_peer_id_observed__key" ON "ServerScoreReport"("reporter_peer_id", "target_peer_id", "observed_at");

-- CreateIndex
CREATE UNIQUE INDEX "User_x25519_public_key_key" ON "User"("x25519_public_key");
