const TOKEN = /[a-z0-9]+/g;

/**
 * Include and exclude keywords match similar wording, not only the exact phrase.
 * A keyword with * / ? or % / _ is a wildcard pattern. Otherwise each word may
 * match a stemmed or slightly misspelled word in the page text.
 */
export function findKeywordMatches(text: string, keywords: string[]) {
  const haystack = text.toLowerCase();
  return keywords.filter((keyword) => keywordMatches(haystack, keyword));
}

function keywordMatches(haystack: string, keyword: string) {
  const pattern = keyword.trim().toLowerCase();
  if (!pattern) return false;
  if (hasWildcard(pattern)) return wildcardMatch(haystack, pattern);
  if (haystack.includes(pattern)) return true;
  return similarMatch(haystack, pattern);
}

function hasWildcard(keyword: string) {
  return /[*?%_]/.test(keyword) && /[^*?%_\s]/.test(keyword);
}

function wildcardMatch(haystack: string, keyword: string) {
  let source = "";
  for (const char of keyword) {
    if (char === "*" || char === "%") source += "[\\s\\S]*";
    else if (char === "?" || char === "_") source += "[a-z0-9]";
    else source += char.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }
  return new RegExp(source, "i").test(haystack);
}

function similarMatch(haystack: string, keyword: string) {
  const wanted = tokens(keyword);
  const page = tokens(haystack);
  if (wanted.length === 0 || page.length === 0) return false;

  if (wanted.length === 1) {
    const word = wanted[0];
    if (page.some((token) => tokensSimilar(word, token))) return true;
    for (let index = 0; index < page.length - 1; index += 1) {
      if (tokensSimilar(word, page[index] + page[index + 1])) return true;
    }
    return false;
  }

  for (let index = 0; index <= page.length - wanted.length; index += 1) {
    const window = page.slice(index, index + wanted.length);
    if (window.every((token, tokenIndex) => tokensSimilar(wanted[tokenIndex], token))) return true;
  }

  const joined = wanted.join("");
  return page.some((token) => tokensSimilar(joined, token));
}

function tokens(value: string) {
  return value.toLowerCase().match(TOKEN) ?? [];
}

function tokensSimilar(left: string, right: string) {
  if (left === right) return true;
  if (stem(left) === stem(right)) return true;
  return withinDistance(left, right) || withinDistance(stem(left), stem(right));
}

function withinDistance(left: string, right: string) {
  const limit = maxDistance(left, right);
  if (limit === 0) return false;
  return levenshtein(left, right, limit) <= limit;
}

function maxDistance(left: string, right: string) {
  const shortest = Math.min(left.length, right.length);
  if (shortest <= 4) return 0;
  if (shortest <= 7) return 1;
  return 2;
}

function levenshtein(left: string, right: string, limit: number) {
  if (Math.abs(left.length - right.length) > limit) return limit + 1;
  if (left === right) return 0;

  let previous = Array.from({ length: right.length + 1 }, (_, index) => index);
  for (let i = 1; i <= left.length; i += 1) {
    const current = [i];
    let rowBest = current[0];
    for (let j = 1; j <= right.length; j += 1) {
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      const value = Math.min(current[j - 1] + 1, previous[j] + 1, previous[j - 1] + cost);
      current[j] = value;
      if (value < rowBest) rowBest = value;
    }
    if (rowBest > limit) return limit + 1;
    previous = current;
  }
  return previous[right.length];
}

export function stem(word: string) {
  let value = word.toLowerCase();
  if (value.length <= 2) return value;

  if (value.endsWith("ies") && value.length > 4) return `${value.slice(0, -3)}y`;

  if (value.endsWith("ing") && value.length > 4) return undouble(value.slice(0, -3));
  if (value.endsWith("ed") && value.length > 3) return undouble(value.slice(0, -2));

  if (/(ches|shes|sses|zzes|xes)$/.test(value) && value.length > 4) return value.slice(0, -2);

  if (value.endsWith("es") && value.length > 4 && !value.endsWith("ss")) {
    return dropSilentE(value.slice(0, -2));
  }

  if (value.endsWith("s") && !value.endsWith("ss") && value.length > 3) {
    value = value.slice(0, -1);
  }

  return dropSilentE(value);
}

function undouble(value: string) {
  if (value.length >= 2 && value.at(-1) === value.at(-2) && !isVowel(value.at(-1) ?? "")) {
    value = value.slice(0, -1);
  }
  return dropSilentE(value);
}

function dropSilentE(value: string) {
  if (value.length > 3 && value.endsWith("e") && !value.endsWith("ee")) return value.slice(0, -1);
  return value;
}

function isVowel(char: string) {
  return "aeiou".includes(char);
}
