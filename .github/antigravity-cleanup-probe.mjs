import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Deferred from "effect/Deferred";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Temp from "./src/provider/AntigravityRuntimeTemp.ts";
import * as Installation from "./src/provider/AntigravityInstallation.ts";

await Effect.runPromise(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-23877-real-" });
  const gc = path.join(root, "userdata", "antigravity-gc");
  const entered = yield* Deferred.make();
  const release = yield* Deferred.make();
  let starts = 0;
  let parentDeletes = 0;
  const trackedFs = FileSystem.FileSystem.of({
    ...fs,
    remove: (target, options) => {
      if (target.startsWith(gc) && options?.recursive) parentDeletes += 1;
      return fs.remove(target, options);
    },
  });
  const temp = yield* Temp.make(gc).pipe(
    Effect.provideService(FileSystem.FileSystem, trackedFs),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, ChildProcessSpawner.make(command =>
      Effect.gen(function* () {
        starts += 1;
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
        return yield* spawner.spawn(command);
      }),
    )),
  );
  const install = yield* Installation.makeAntigravityInstallation({ baseDir: root }).pipe(
    Effect.provideService(Temp.AntigravityRuntimeTemp, temp),
    Effect.provideService(FileSystem.FileSystem, trackedFs),
  );
  const marker = path.join(root, "request-serving.txt");
  yield* fs.writeFileString(marker, "responsive");
  yield* install.start;
  const terminal = yield* install.changes.pipe(
    Stream.filter(state => ["succeeded", "failed", "cancelled"].includes(state.phase)),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );
  console.log("Official runtime validation/install:", JSON.stringify(terminal));
  if (terminal.phase !== "succeeded") throw new Error("Real managed install failed");
  yield* Deferred.await(entered);
  const entries = yield* fs.readDirectory(gc, { recursive: true });
  console.log("Retained extraction entries before cleanup:", entries.length);
  console.log("Deepest retained path length:", Math.max(...entries.map(entry => path.join(gc, entry).length)));
  console.log("Unrelated server read while cleanup is held:", yield* fs.readFileString(marker));
  console.log("Cleanup workers started:", starts, "parent recursive unpack deletions:", parentDeletes);
  yield* Deferred.succeed(release, undefined);
  yield* temp.drain;
  const remaining = yield* fs.readDirectory(gc);
  console.log("After cleanup, pending runtime directories:", remaining.length);
  console.log("Active managed version:", (yield* install.resolve()).version);
  if (remaining.length !== 0 || parentDeletes !== 0 || starts !== 1) throw new Error("Cleanup proof failed");
}).pipe(Effect.scoped, Effect.provide(NodeServices.layer), Effect.provide(NodeHttpClient.layerFetch)));
