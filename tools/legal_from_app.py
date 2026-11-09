#!/usr/bin/env python3
"""Copy the app's legal documents into the server, for its web pages.

The Terms, Privacy Policy and Community Guidelines are written once, in the
app (Frontend: lib/pages/legal_texts.dart). The app shows them inside it;
the server shows the same words at /terms, /privacy and
/community-guidelines, because the app stores ask for web addresses.

This reads the app's file and writes legal/texts.json here, which the
server builds in (legal_pages.go). Run it whenever the app's texts change:

    python3 tools/legal_from_app.py ../Frontend/lib/pages/legal_texts.dart

and commit legal/texts.json. legal_pages_test.go checks the copy is whole.
"""
import json
import re
import sys

src = open(sys.argv[1], encoding="utf-8").read()
consts = open(sys.argv[1].replace("pages/legal_texts.dart", "config/constants.dart"), encoding="utf-8").read()


def const(name):
    m = re.search(r"static const String " + name + r"\s*=\s*'([^']*)';", consts)
    if not m:
        sys.exit("constants.dart has no " + name)
    return m.group(1)


officer_name = const("grievanceOfficer")
values = {
    "_name": const("appName"),
    "_support": const("supportEmail"),
    "_privacy": const("privacyEmail"),
    "_grievance": const("grievanceEmail"),
    "_officer": ("our Grievance Officer, " + officer_name + ",") if officer_name else "our Grievance Officer",
}


def strings_in(text):
    """Every Dart string literal in text, in order, adjacent ones joined."""
    out, i, cur, last_end = [], 0, None, 0
    while i < len(text):
        c = text[i]
        if c in "'\"":
            q, j, buf = c, i + 1, []
            while text[j] != q:
                if text[j] == "\\":
                    buf.append(text[j + 1])
                    j += 2
                    continue
                if text[j] == "$":
                    m = re.match(r"\$\{?([A-Za-z_][A-Za-z_0-9]*)\}?", text[j:])
                    key = m.group(1)
                    if key not in values:
                        sys.exit("unknown value in a legal text: $" + key)
                    buf.append(values[key])
                    j += m.end()
                    continue
                buf.append(text[j])
                j += 1
            s = "".join(buf)
            between = text[last_end:i]
            if cur is not None and between.strip() == "":
                cur += s  # 'a' 'b' is one string
            else:
                if cur is not None:
                    out.append(cur)
                cur = s
            i = j + 1
            last_end = i
            continue
        i += 1
    if cur is not None:
        out.append(cur)
    return out


def block(name):
    m = re.search(r"const " + name + r" = LegalDocument\((.*?)\n\);", src, re.S)
    if not m:
        sys.exit("legal_texts.dart has no " + name)
    return m.group(1)


def field(body, key):
    m = re.search(r"\n  " + key + r": (.*?)(?=\n  [a-z]+: |\Z)", body, re.S)
    return m.group(1)


def plain_value(text):
    """A field that is a string, or one of the named values (_support)."""
    found = strings_in(text)
    if found:
        return found[0]
    key = text.strip().rstrip(",")
    if key not in values:
        sys.exit("unknown value in a legal text: " + key)
    return values[key]


def document(name):
    body = block(name)
    sections = []
    for sm in re.finditer(r"LegalSection\((.*?)\]\),", field(body, "sections"), re.S):
        parts = strings_in(sm.group(1))
        sections.append({"title": parts[0], "body": parts[1:]})
    return {
        "title": strings_in(field(body, "title"))[0],
        "intro": strings_in(field(body, "intro"))[0],
        "summary": strings_in(field(body, "summary")),
        "sections": sections,
        "contact": plain_value(field(body, "contact")),
    }


updated = re.search(r"const legalUpdated = '([^']+)';", src).group(1)
out = {
    "updated": updated,
    "terms": document("termsOfService"),
    "privacy": document("privacyPolicy"),
    "guidelines": document("communityGuidelines"),
}
with open("legal/texts.json", "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=1)
    f.write("\n")
print("legal/texts.json: updated", updated, "-",
      ", ".join(f"{k} {len(v['sections'])} sections" for k, v in out.items() if k != "updated"))
