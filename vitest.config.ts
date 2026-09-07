import { defineConfig } from 'vitest/config';

// There are no unit tests in this repository, and that is deliberate rather
// than an omission: every guarantee this library makes is about what Postgres
// and a broker do under concurrency, and a mock of those cannot fail in the
// ways the library exists to prevent.
//
// This config stays so `npm run test:unit` has somewhere to look when the first
// pure function arrives. `npm test` runs the integration suite, which is the
// suite. It needs `npm run infra:up` first.
export default defineConfig({ test: { include: ['src/**/*.test.ts'], passWithNoTests: true } });
