import { cp } from 'node:fs/promises'

for (const client of ['api-users', 'api-media', 'users']) {
  await cp(
    new URL(`../src/lib/prisma/${client}/__generated__/`, import.meta.url),
    new URL(`../dist/lib/prisma/${client}/__generated__/`, import.meta.url),
    { recursive: true }
  )
}
