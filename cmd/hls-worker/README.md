# How the app works out what a video is

Every video uploaded to Battle Arena gets inspected by this worker while it
still has the file. This page says what that inspection does, how to switch
the clever half on, and — the part that matters most — what happens when it is
switched off.

---

## The short version

The worker does four things to every video, in order:

| Pass | What it does | Needs |
|---|---|---|
| **shape** | Measures the file: how often it cuts, how loud it is, how much of it is silence, how bright it is | ffmpeg only |
| **text** | Reads any words written **on the screen** | ffmpeg + tesseract |
| **speech** | Writes down what is **said out loud** | whisper.cpp |
| **understand** | Reads those words and decides what the video is **about** | a language model |
| **frames** | **Looks** at the video, for the ones that say nothing | the same model + a vision file |

The first three are measurements. The last two are the ones that need a model,
and they are the ones the settings below turn on.

---

## Where do these get set? (read this first)

**On the machine that runs the transcode worker — not on your API server.**

Your backend on Render never touches these. It does not have the model, does
not run llama.cpp, and does not need to: it only reads the results out of the
database. Setting them there does nothing at all.

Your worker is **GitHub Actions**. `.github/workflows/hls-worker.yml` runs
every half hour and whenever the backend pokes it after an upload, and that
workflow already builds llama.cpp, downloads the model, proves it runs, and
exports all four variables into the job.

So, concretely:

| Where you run the worker | What you have to do |
|---|---|
| **GitHub Actions** (what you use today) | **Nothing.** Already set up in `hls-worker.yml` |
| Your own laptop, for testing | Export the four variables — recipe below |
| A VM or container you run yourself | Same recipe, in the service's environment |

They are **not** repo secrets. Nothing here is a password, so none of it goes
in Settings → Secrets. They are just file paths on whichever machine is doing
the work, and the workflow writes them into `$GITHUB_ENV` at the point it has
proved the files work.

### How to check whether it is on right now

Open any recent `hls-worker` run in the **Actions** tab and look at the step
called **"check the understanding model works"**. Its last lines say so
outright:

```
understanding: ON        ← reading works
looking: ON              ← silent videos work too
```

If you do not see those lines, the step failed and the worker ran without a
model. That is **not** a broken build — the step is `continue-on-error` on
purpose, because a model that will not start must cost you tags and never the
whole transcode queue. The videos from that run get the keyword fallback
described further down.

The second place to look is the video itself, on the admin analysis page: its
`passes` list says which passes actually ran. `understand` or `frames` in that
list means a model looked at it.

---

## The four settings

```
UNDERSTAND_BIN            the llama.cpp program that reads words
UNDERSTAND_MODEL          the model file it reads them with
UNDERSTAND_FRAMES_BIN     the llama.cpp program that looks at pictures
UNDERSTAND_PROJECTOR      the extra file that lets it see
```

They come in two pairs, on purpose.

- Set the **first pair** and the worker can read a transcript and say what the
  video is about.
- Set the **second pair** as well and it can also look at videos that never
  say anything.

If only the first pair is set, reading works and looking is off. Nothing
breaks — the silent videos simply get no answer from a model, exactly as
before either pair existed.

### Where the model comes from

**Qwen3-VL-4B-Instruct**, run locally through
[llama.cpp](https://github.com/ggml-org/llama.cpp). It is Apache 2.0
licensed, so it is free to use commercially. There is no API key, no bill, no
rate limit, and no company that can change the terms.

Two files are downloaded from Hugging Face:

| File | Size | What it is |
|---|---|---|
| `Qwen3VL-4B-Instruct-Q4_K_M.gguf` | ~2.4 GB | the model itself |
| `mmproj-Qwen3VL-4B-Instruct-Q8_0.gguf` | ~454 MB | the vision projector — turns pixels into something the model can read |

It is a **vision** model even though the reading pass only handles words. That
was the point: most of this catalogue is silent, so being able to *look* had to
come from the same download rather than a second model.

### On GitHub Actions — already done

`.github/workflows/hls-worker.yml` builds llama.cpp, downloads both files,
caches them, **proves they run**, and then exports all four variables. You do
not have to do anything. If you want a bigger or smaller model, change
`UNDERSTAND_MODEL_FILE` at the top of that file — the name goes into both the
download URL and the cache key, so it re-fetches once and then reuses.

The check step is `continue-on-error`, and deliberately so: a model that will
not run must cost you tags, never the whole transcode queue.

### On your own machine

```bash
git clone --depth 1 --branch b10819 https://github.com/ggml-org/llama.cpp /tmp/llama-src
cmake -S /tmp/llama-src -B /tmp/llama-build \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DLLAMA_CURL=OFF -DLLAMA_BUILD_TESTS=OFF
cmake --build /tmp/llama-build --config Release -j 4 --target llama-cli llama-mtmd-cli

mkdir -p ~/llama
cp /tmp/llama-build/bin/llama-cli      ~/llama/llama-cli
cp /tmp/llama-build/bin/llama-mtmd-cli ~/llama/llama-mtmd-cli

REPO=Qwen/Qwen3-VL-4B-Instruct-GGUF
curl -L -o ~/llama/model.gguf  "https://huggingface.co/$REPO/resolve/main/Qwen3VL-4B-Instruct-Q4_K_M.gguf"
curl -L -o ~/llama/mmproj.gguf "https://huggingface.co/$REPO/resolve/main/mmproj-Qwen3VL-4B-Instruct-Q8_0.gguf"

export UNDERSTAND_BIN=~/llama/llama-cli
export UNDERSTAND_MODEL=~/llama/model.gguf
export UNDERSTAND_FRAMES_BIN=~/llama/llama-mtmd-cli
export UNDERSTAND_PROJECTOR=~/llama/mmproj.gguf
```

**Two traps worth knowing before you debug anything.**

1. **`-j 4`, with a number.** A bare `-j` means unlimited parallel compiles.
   llama.cpp has hundreds of large C++ files that each want about a gigabyte,
   and an unlimited build exhausts a 16 GB machine. It gets killed with exit
   143, which reads like somebody cancelled the job rather than like the build
   asking for too much.

2. **Never add `-DLLAMA_BUILD_SERVER=OFF`.** It looks like an obvious saving
   and it deletes `llama-cli`, which is the binary this whole feature runs on.
   That shipped once and stopped the transcode queue for three hours.

---

## What happens when it is NOT set

**The app still answers. It just answers worse, and it never says so out loud.**

With no model, the worker falls back to a **keyword list** run over the words
it already collected — what was said out loud and what was written on screen
(`keywordTags` in `analyze.go`). So `auto_tags` is still filled, and the
ranker still gets a machine opinion to weigh against the creator's.

The difference is that a word list does not understand anything. It looks for
spellings. Two real failures from this app:

- A video about *nobody knows what the future holds*. The Hindi word for
  future is in the list, but whisper spelled it with one letter different, so
  the video earned nothing. Any person reading the sentence could see what it
  was about.
- A video about reciting the Hanuman Chalisa when something frightens you was
  filed as **dance** — because with the vowel marks stripped, "हनुमान चालीसा"
  shares consonants with the Hindi word for dance. Two unrelated phrases, one
  string of letters.

Neither is fixable by adding more words to the list.

And if the video is **silent**, a keyword list has nothing at all to read. With
no model, those videos get no machine opinion, and the category falls back to
matching keywords against the title the creator typed.

### How to tell which one happened

Every stored analysis has a `passes` list saying which passes actually **ran**
— not which found something. Those are different facts, and the difference is
the whole reason the field exists: a video with nobody talking and a worker
with no whisper installed both produce an empty transcript, and only one of
those is a problem.

```
passes: ["shape","text","speech","understand"]   the model read it
passes: ["shape","frames"]                       silent, the model looked at it
passes: ["shape","text","speech"]                no model — keyword list only
```

You can see this per video on the admin analysis page, which also shows the
model's category, the creator's category, and whether they disagree.

---

## Silent videos

Most of this catalogue is silent. Of 114 videos, 79 produce no transcript at
all, and 62 of those have no readable text on screen either.

Reading cannot help any of them, so the worker **looks** instead: four stills,
spread across the video, scaled to 256 pixels wide, shown to the same model
(`understand_frames.go`). It is asked the same question against the same
category list, and its answer goes through the same validation, so it can no
more invent a category than the reading pass can.

It also writes down **topics** — short free-text phrases for what it can see,
like "street food" or "temple doorway". Those are not limited to the eighteen
categories, which matters most here: for a silent video they are the only
words that will ever exist about it.

> **Why 256 pixels.** Turning a picture into something the model can read is
> the expensive part, and the cost is a cliff, not a slope. Measured on the
> same image: 512 wide took 18,675ms, 256 wide took 1,549ms — twelve times
> cheaper for four times fewer pixels. And it costs nothing that matters; the
> small version read a title card *more* plainly than the large one. If frames
> ever need to be bigger, measure again before raising it.

---

## What about videos uploaded before all this existed?

They are not left behind. On the next boot after this change, the backend
walks the videos that already exist and settles what it can
(`backfill_category_provenance.go`). It runs once and records itself, so it
never repeats.

Two different jobs there, and they are very different in how much they can
recover:

- **What the model concluded: fully recoverable.** Those videos were already
  watched — their conclusion has been sitting in `auto_tags` the whole time,
  and nothing ever turned it into a category. The backfill carries it the last
  few inches. No guessing involved.

- **What the creator said: mostly NOT recoverable, on purpose.** The old app
  always sent a category (its dropdown defaulted to "other"), so a stored
  category came either from a person choosing, or from the server deriving it.
  Nothing recorded which. The backfill only claims a creator pick where it can
  be *proven* by elimination: the creator's tags name one category and the row
  stores a different one, which the server's own derivation could never have
  produced. Everything else stays empty, because "we do not know" is the true
  answer and a column meaning "a person said so" must never hold anything
  else.

It deliberately never re-runs the keyword matcher to decide this. That table
has been corrected at least once — four of its answers were not categories at
all, and ten real categories had no keywords and were unreachable — so
re-running today's rules over a row written under the old ones would produce a
different answer for reasons that have nothing to do with the creator, and
record it as a human claim nobody made.

---

## What happens to the answer

Once a video has been watched, `settleCategory` (in the backend's
`video_analysis.go`) writes the verdict onto the row itself:

| Column | Meaning |
|---|---|
| `category` | the app's single best answer |
| `creator_category` | **only** what a person chose. Empty means they did not choose |
| `machine_category` | what the model concluded. Empty means it looked and could not tell |
| `category_source` | `agreed`, `machine`, `creator` or `guess` |

The model's answer replaces `category` **only when it actually had an
opinion**. If it looked and could not tell, whatever upload decided stays,
because that answer had the creator's own pick behind it and a fresh keyword
guess at the same title would be a downgrade.

`creator_category` is never written by the worker. It is the one column here
that holds a claim made by a person, and no machine gets to write to it.
