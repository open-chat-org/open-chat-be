-- CreateTable
CREATE TABLE "User" (
    "public_key" TEXT NOT NULL,
    "username" TEXT,
    "name" TEXT,
    "quote" TEXT,
    "phone" TEXT,
    "email" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL
);

-- CreateIndex
CREATE UNIQUE INDEX "User_public_key_key" ON "User"("public_key");

-- CreateIndex
CREATE UNIQUE INDEX "User_username_key" ON "User"("username");
