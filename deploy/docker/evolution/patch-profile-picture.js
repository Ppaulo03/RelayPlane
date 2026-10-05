// Evolution 2.3.7 fetches the profile picture of the chat of EVERY message it handles (messages.upsert, also the ones we send) and of every
// contact update, with `await` and no timeout of its own, inside a sequential loop. When WhatsApp never answers that query (Baileys itself
// documents a case in which "the server never responds") the Baileys default query timeout, 60 s, passes before the next message is handled:
// messages sent one after the other show up 60 s apart. RelayPlane never uses profile pictures, so the fetch is OFF by default; set
// RELAYPLANE_PROFILE_PICTURE_TIMEOUT_MS to a few thousand to turn it back on with a bound.
//
// This is a textual patch of the pinned build: it must match EXACTLY once, otherwise the image build fails (a new Evolution version needs this
// reviewed, see docs/runbooks/PROVIDER-UPGRADE.md).
const fs = require("fs");
const cp = require("child_process");

const file = "/evolution/dist/main.js"; // `node dist/main` (package.json "main")
const from =
  'async profilePicture(e){let t=G(e);try{let o=await this.client.profilePictureUrl(t,"image");return{wuid:t,profilePictureUrl:o}}catch{return{wuid:t,profilePictureUrl:null}}}';
const to =
  'async profilePicture(e){let t=G(e);const ms=Number(process.env.RELAYPLANE_PROFILE_PICTURE_TIMEOUT_MS||0);if(!(ms>0))return{wuid:t,profilePictureUrl:null};try{let o=await this.client.profilePictureUrl(t,"image",ms);return{wuid:t,profilePictureUrl:o}}catch{return{wuid:t,profilePictureUrl:null}}}';

const src = fs.readFileSync(file, "utf8");
const n = src.split(from).length - 1;
if (n !== 1) {
  console.error(`profilePicture() was found ${n} times in ${file}, expected exactly 1: this Evolution build changed, review the patch`);
  process.exit(1);
}
fs.writeFileSync(file, src.replace(from, () => to));
cp.execFileSync("node", ["--check", file], { stdio: "inherit" }); // the bundle must still parse
console.log("profilePicture() is bounded and off by default");
