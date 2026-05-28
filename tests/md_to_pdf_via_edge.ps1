# Convert source Markdown files to PDF using pandoc (MD->HTML) + Edge headless (HTML->PDF).
# No LaTeX or Word dependency.

$pandoc = 'C:\Users\Spencer\AppData\Local\Pandoc\pandoc.exe'
$edge   = 'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
$root   = Split-Path -Parent $PSScriptRoot   # project root (this script lives in tests/)
$sub    = Join-Path $root 'submission'

# Simple, clean CSS for the HTML (Word-like print style).
$css = @'
<style>
  body { font-family: Calibri, Arial, sans-serif; font-size: 11pt; line-height: 1.4; max-width: 7.5in; margin: 0.5in auto; color: #222; }
  h1 { font-size: 20pt; border-bottom: 2px solid #444; padding-bottom: 4pt; }
  h2 { font-size: 14pt; margin-top: 18pt; color: #333; }
  h3 { font-size: 12pt; margin-top: 14pt; color: #555; }
  table { border-collapse: collapse; margin: 8pt 0; font-size: 10pt; }
  th, td { border: 1px solid #aaa; padding: 4pt 8pt; text-align: left; vertical-align: top; }
  th { background: #eee; }
  code { font-family: Consolas, "Courier New", monospace; font-size: 9.5pt; background: #f4f4f4; padding: 1pt 3pt; border-radius: 2pt; }
  pre { background: #f4f4f4; padding: 8pt; border-left: 3px solid #888; overflow-x: auto; font-size: 9pt; }
  pre code { background: none; padding: 0; }
  img { max-width: 100%; }
  hr { border: 0; border-top: 1px solid #ccc; margin: 16pt 0; }
</style>
'@

function Convert-MarkdownToPdf {
    param([string]$mdPath, [string]$pdfPath)

    $htmlPath = [System.IO.Path]::ChangeExtension($pdfPath, 'html')

    # MD -> HTML (standalone, with embedded CSS).
    & $pandoc $mdPath -o $htmlPath --standalone --metadata title='' --resource-path="$root;$sub"
    # Inject our CSS just after <head>.
    $html = Get-Content $htmlPath -Raw
    $html = $html -replace '</head>', ($css + '</head>')
    Set-Content -Path $htmlPath -Value $html -Encoding UTF8

    # HTML -> PDF via Edge headless.
    $fileUri = 'file:///' + $htmlPath.Replace('\', '/')
    & $edge --headless --disable-gpu --no-pdf-header-footer --print-to-pdf="$pdfPath" $fileUri 2>$null
    # Edge can return before the file is fully written - wait briefly.
    Start-Sleep -Seconds 2
    Remove-Item $htmlPath -Force -ErrorAction SilentlyContinue

    if (Test-Path $pdfPath) {
        $size = (Get-Item $pdfPath).Length
        Write-Output ("  OK -> {0} ({1:N1} KB)" -f [System.IO.Path]::GetFileName($pdfPath), ($size/1KB))
    } else {
        Write-Output ("  FAILED to create {0}" -f $pdfPath)
    }
}

Write-Output "Converting schema documentation..."
Convert-MarkdownToPdf -mdPath (Join-Path $root 'docs\schema.md') -pdfPath (Join-Path $sub '1_schema_documentation.pdf')

Write-Output "Converting ER diagram doc..."
# Re-create the er_diagram_doc.md we deleted earlier (just for this conversion).
$erMd = @'
# ER Diagram - Box Office vs. Ratings

**Project:** Box Office vs. Ratings - Movie Pipeline
**Author:** Spencer Goss

## Diagram

![ER Diagram](er_diagram.png)

## Entities

The schema has three tables resolving a many-to-many relationship between films and genres.

- **FILMS** - one row per movie. Surrogate primary key `film_id`; natural business key `tmdb_id` enforces uniqueness across reloads.
- **GENRES** - TMDB's genre lookup. The TMDB id is reused as the primary key so the join table stays stable across re-extractions.
- **FILM_GENRES** - many-to-many resolution table. Composite primary key on `(film_id, genre_id)` prevents duplicate pairings.

## Cardinality

- One **film** is categorized as **one or more genres** (resolved through `FILM_GENRES`).
- One **genre** applies to **zero or more films** (resolved through `FILM_GENRES`).

## Referential integrity

- `FILM_GENRES.film_id` -> `FILMS.film_id` **ON DELETE CASCADE** - deleting a film removes its genre links automatically.
- `FILM_GENRES.genre_id` -> `GENRES.genre_id` **ON DELETE RESTRICT** - a genre cannot be deleted while films still reference it.

## Why this shape (not a flat table)

A single `films` table with a comma-separated `genres` text column would:

- violate 1NF (cells are not atomic),
- make "all films in genre X" a linear scan with `LIKE '%X%'` (no index help),
- allow spelling drift ("Sci-Fi" vs "Science Fiction") across rows.

Splitting genres into a lookup table plus a many-to-many join table makes the schema 3NF, supports indexed genre queries, and centralizes the canonical genre name in one place.
'@
$erMdPath = Join-Path $sub 'er_diagram_doc.md'
Set-Content -Path $erMdPath -Value $erMd -Encoding UTF8
Convert-MarkdownToPdf -mdPath $erMdPath -pdfPath (Join-Path $sub '2_er_diagram.pdf')
Remove-Item $erMdPath -Force -ErrorAction SilentlyContinue

Write-Output ""
Write-Output "Final submission folder:"
Get-ChildItem $sub | Sort-Object Name | Select-Object Name, Length
