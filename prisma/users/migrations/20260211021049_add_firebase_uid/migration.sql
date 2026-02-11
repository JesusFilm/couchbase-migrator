/*
  Warnings:

  - A unique constraint covering the columns `[firebaseUserId]` on the table `User` will be added. If there are existing duplicate values, this will fail.

*/
-- AlterTable
ALTER TABLE "User" ADD COLUMN "firebaseUserId" TEXT;

-- CreateIndex
CREATE UNIQUE INDEX "User_firebaseUserId_key" ON "User"("firebaseUserId");

-- CreateIndex
CREATE INDEX "User_firebaseUserId_idx" ON "User"("firebaseUserId");
