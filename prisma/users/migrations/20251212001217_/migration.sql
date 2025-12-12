-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_User" (
    "ownerId" TEXT NOT NULL PRIMARY KEY,
    "email" TEXT NOT NULL,
    "ssoGuid" TEXT NOT NULL,
    "coreId" TEXT NOT NULL,
    "isSecondaryAccount" BOOLEAN NOT NULL DEFAULT false
);
INSERT INTO "new_User" ("coreId", "email", "ownerId", "ssoGuid") SELECT "coreId", "email", "ownerId", "ssoGuid" FROM "User";
DROP TABLE "User";
ALTER TABLE "new_User" RENAME TO "User";
CREATE UNIQUE INDEX "User_email_key" ON "User"("email");
CREATE UNIQUE INDEX "User_ssoGuid_key" ON "User"("ssoGuid");
CREATE UNIQUE INDEX "User_coreId_key" ON "User"("coreId");
CREATE INDEX "User_email_idx" ON "User"("email");
CREATE INDEX "User_ssoGuid_idx" ON "User"("ssoGuid");
CREATE INDEX "User_coreId_idx" ON "User"("coreId");
CREATE UNIQUE INDEX "User_ownerId_email_ssoGuid_coreId_key" ON "User"("ownerId", "email", "ssoGuid", "coreId");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
