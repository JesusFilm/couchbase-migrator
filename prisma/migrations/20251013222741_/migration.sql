-- CreateTable
CREATE TABLE "users" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "theKeySsoGuid" TEXT NOT NULL,
    "theKeyGuid" TEXT NOT NULL,
    "theKeyRelayGuid" TEXT NOT NULL,
    "theKeyGrPersonId" TEXT,
    "email" TEXT NOT NULL,
    "nameFirst" TEXT NOT NULL,
    "nameLast" TEXT NOT NULL,
    "homeCountry" TEXT,
    "notificationCountries" TEXT NOT NULL DEFAULT '[]',
    "createdAt" DATETIME NOT NULL,
    "updatedAt" DATETIME NOT NULL,
    "cas" BIGINT NOT NULL,
    "syncRev" TEXT NOT NULL,
    "syncSequence" INTEGER NOT NULL,
    "syncRecentSequences" TEXT NOT NULL DEFAULT '[]',
    "syncTimeSaved" TEXT NOT NULL,
    "ingestedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "playlists" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "owner" TEXT NOT NULL,
    "playlistName" TEXT NOT NULL,
    "playlistByDisplayName" TEXT,
    "note" TEXT NOT NULL DEFAULT '',
    "noteModifiedAt" DATETIME,
    "createdAt" DATETIME,
    "updatedAt" DATETIME,
    "cas" BIGINT NOT NULL,
    "syncRev" TEXT NOT NULL,
    "syncSequence" INTEGER NOT NULL,
    "syncRecentSequences" TEXT NOT NULL DEFAULT '[]',
    "syncTimeSaved" TEXT NOT NULL,
    "ingestedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "playlist_items" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "playlistId" TEXT NOT NULL,
    "createdAt" DATETIME NOT NULL,
    "languageId" INTEGER NOT NULL,
    "mediaComponentId" TEXT NOT NULL,
    "type" TEXT,
    CONSTRAINT "playlist_items_playlistId_fkey" FOREIGN KEY ("playlistId") REFERENCES "playlists" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateIndex
CREATE UNIQUE INDEX "users_theKeySsoGuid_key" ON "users"("theKeySsoGuid");
