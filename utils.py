import os
import pandas as pd

def load_language_corpus(csv_paths):
    """
    Reads multiple CSVs for one language and concatenates them into one dataframe.
    Assumes CSVs have columns: book, chapter, verse, text
    """
    dfs = []
    for path in csv_paths:
        df = pd.read_csv(path)
        dfs.append(df)
    combined = pd.concat(dfs, ignore_index=True)
    # sort to ensure order is correct
    combined = combined.sort_values(["book", "chapter", "verse_number"]).reset_index(drop=True)
    return combined

def create_parallel_corpora(lang_files, pairs, output_dir):
    """
    lang_files: dict mapping lang_name -> list of 3 csv file paths
    pairs: list of tuples [(lang1, lang2), ...] defining which corpora to create
    output_dir: where to save the parallel corpora
    """
    os.makedirs(output_dir, exist_ok=True)

    corpora = {}
    for lang, files in lang_files.items():
        corpora[lang] = load_language_corpus(files)

    for lang1, lang2 in pairs:
        df1 = corpora[lang1]
        df2 = corpora[lang2]

        # 1:1 merge
        merged = pd.merge(df1, df2, on=["book", "chapter", "verse_number"], suffixes=(f"_{lang1}", f"_{lang2}"))

        merged = merged.rename(columns={"verse_number": "verse"})
        merged = merged[["book", "chapter", "verse", f"text_{lang1}", f"text_{lang2}"]]

        out_path = os.path.join(output_dir, f"{lang1}_{lang2}_parallel.csv")
        merged.to_csv(out_path, index=False, encoding="utf-8")
        print(f"Saved: {out_path}")