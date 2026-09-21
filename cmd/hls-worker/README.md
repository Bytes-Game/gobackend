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
