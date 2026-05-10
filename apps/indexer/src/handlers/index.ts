// Side-effect import — each handler module calls register() at top
// level. Importing this barrel from the worker once boots the registry
// with every built-in handler. Adding a new event type is then
// `import "./handlers/my-event.js"` here, no sync.ts edit.

import "./erc20.js";
import "./erc721.js";
import "./erc1155.js";

export { dispatch, register, type EventHandler, type DecodedLogContext } from "./registry.js";
