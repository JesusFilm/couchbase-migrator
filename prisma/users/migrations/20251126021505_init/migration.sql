-- CreateTable
CREATE TABLE "User" (
    "ownerId" TEXT NOT NULL PRIMARY KEY,
    "email" TEXT NOT NULL,
    "ssoGuid" TEXT NOT NULL,
    "coreId" TEXT NOT NULL
);

-- CreateIndex
CREATE UNIQUE INDEX "User_email_key" ON "User"("email");

-- CreateIndex
CREATE UNIQUE INDEX "User_ssoGuid_key" ON "User"("ssoGuid");

-- CreateIndex
CREATE UNIQUE INDEX "User_coreId_key" ON "User"("coreId");

-- CreateIndex
CREATE INDEX "User_email_idx" ON "User"("email");

-- CreateIndex
CREATE INDEX "User_ssoGuid_idx" ON "User"("ssoGuid");

-- CreateIndex
CREATE INDEX "User_coreId_idx" ON "User"("coreId");

-- CreateIndex
CREATE UNIQUE INDEX "User_ownerId_email_ssoGuid_coreId_key" ON "User"("ownerId", "email", "ssoGuid", "coreId");
