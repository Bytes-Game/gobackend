package main

import (
	"strings"
	"testing"
)

// tsvLine builds one tesseract tsv row. The real format is 12 tab-separated
// columns; only confidence (11) and text (12) matter here.
func tsvLine(conf, text string) string {
	return strings.Join([]string{
		"5", "1", "1", "1", "1", "1", "0", "0", "10", "10", conf, text,
	}, "\t")
}

// ════════════════════════════════════════════════════════════════════════════
// READING THE CAPTION WITHOUT READING THE GRAIN
// ════════════════════════════════════════════════════════════════════════════
//
// Without a confidence filter this pass produced almost pure noise. Real
// stored readings from production:
//
//	"—_ Vale 7 SCs Pda gy? Ww | C 0 — "A My ¥ ROMAN waN Sf" THE NEV Mima"
//	"Rees 43 "s malas — — < " \. f = & p« Bay SSS 4 "~"
//
// -psm 11 hunts for text anywhere on the frame, which is right for a caption
// that could be anywhere and also means grain and foliage get read as
// letters. Measured on rendered frames: a readable caption scores 88-96, and
// noise off a grainy frame mostly under 50, highest seen 62.

func TestScreenText_KeepsAConfidentCaption(t *testing.T) {
	tsv := strings.Join([]string{
		"level\tpage\tblock\tpar\tline\tword\tleft\ttop\twidth\theight\tconf\ttext",
		tsvLine("96", "Business"),
		tsvLine("96", "is"),
		tsvLine("88", "cooking"),
	}, "\n")

	got := confidentWords(tsv)
	// "is" is two characters and is dropped for length, not confidence.
	want := []string{"Business", "cooking"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestScreenText_DropsWordsTesseractIsUnsureOf(t *testing.T) {
	// The exact shape of the production noise: many short low-confidence
	// fragments off a grainy frame.
	tsv := strings.Join([]string{
		"level\tpage\tblock\tpar\tline\tword\tleft\ttop\twidth\theight\tconf\ttext",
		tsvLine("5", "Vale"),
		tsvLine("44", "ROMAN"),
		tsvLine("62", "Mima"),
		tsvLine("23", "malas"),
	}, "\n")

	if got := confidentWords(tsv); len(got) != 0 {
		t.Errorf("kept %v. Every one of those is below the threshold, and "+
			"they are what the pass was storing instead of captions.", got)
	}
}

func TestScreenText_IgnoresStructureRowsAndBadNumbers(t *testing.T) {
	// tesseract emits rows for page, block, paragraph and line that carry no
	// text, plus a -1 confidence on them. None of that is a word.
	tsv := strings.Join([]string{
		"level\tpage\tblock\tpar\tline\tword\tleft\ttop\twidth\theight\tconf\ttext",
		tsvLine("-1", ""),
		tsvLine("-1", "   "),
		"short\trow",
		"",
		tsvLine("not-a-number", "recipe"),
		tsvLine("91", "recipe"),
	}, "\n")

	got := confidentWords(tsv)
	if len(got) != 1 || got[0] != "recipe" {
		t.Errorf("got %v, want exactly [recipe]", got)
	}
}

func TestScreenText_ThresholdSitsInTheGapWeMeasured(t *testing.T) {
	// A guard on the number itself. Real captions measured 88-96 and noise
	// topped out at 62, so anything from roughly 65 to 85 is defensible.
	// Outside that the threshold has stopped meaning what this file says.
	if screenTextMinConfidence < 55 {
		t.Errorf("threshold %d is at or under the noise ceiling measured "+
			"(62); the pass goes back to storing grain",
			screenTextMinConfidence)
	}
	if screenTextMinConfidence > 85 {
		t.Errorf("threshold %d is inside the range real captions score "+
			"(88-96); readable text will start being thrown away",
			screenTextMinConfidence)
	}
}
