#!/usr/bin/env python3
# main_gui.py

import sys
import argparse

from PyQt5.QtWidgets import QApplication
try:
    from GUI import BigDataGUI
except Exception as e:
    print("ERROR: cannot import BigDataGUI from GUI.py:", e)
    raise

try:
    import fetch_data
except Exception:
    fetch_data = None


def safe_str(x):
    """Convert value to string safely."""
    if x is None:
        return ""
    return str(x)


def fill_from_fetch_funcs(window, articles_limit, spotify_limit):
    """
    Fallback: use fetch_data functions and GUI's _add_* helpers to populate tables.
    This is used only when GUI does not provide dedicated loader methods.
    """
    if fetch_data is None:
        print("fetch_data module not found. Cannot load data in fallback mode.")
        return

    if hasattr(window, "_add_vnexpress_row") and hasattr(fetch_data, "get_articles_data"):
        try:
            rows = fetch_data.get_articles_data(limit=articles_limit)
            try:
                window.vnexpress_table.setRowCount(0)
            except Exception:
                pass
            for r in rows:
                idv = r.get("id") or r.get("ID") or ""
                title = r.get("title") or r.get("Title") or ""
                published = r.get("published_at") or r.get("publishedAt") or r.get("published") or "-"
                category = r.get("category") or "-"
                summary = r.get("summary") or "-"
                source = r.get("source") or "-"
                window._add_vnexpress_row(safe_str(idv), safe_str(title), safe_str(published),
                                          safe_str(category), safe_str(summary), safe_str(source))
            try:
                window.status.showMessage(f"Loaded {len(rows)} Articles (fallback).")
            except Exception:
                pass
        except Exception as e:
            print("Error while loading articles (fallback):", e)

    if hasattr(window, "_add_song_row") and hasattr(fetch_data, "get_spotify_data"):
        try:
            rows = fetch_data.get_spotify_data(limit=spotify_limit)
            try:
                window.nhaccuatui_table.setRowCount(0)
            except Exception:
                pass
            for r in rows:
                idv = r.get("id") or ""
                date = r.get("date") or "-"
                region = r.get("region") or "-"
                chart_type = r.get("chart_type") or "-"
                rank = r.get("rank") or "-"
                prev_rank = r.get("previous_rank") or r.get("previousRank") or "-"
                rank_delta = r.get("rank_delta") or "-"
                movement = r.get("movement") or "-"
                track_name = r.get("track_name") or r.get("trackName") or r.get("name") or ""
                artists = r.get("artists") or "-"
                release_date = r.get("release_date") or "-"
                window._add_song_row(
                    safe_str(idv), safe_str(date), safe_str(region), safe_str(chart_type),
                    safe_str(rank), safe_str(prev_rank), safe_str(rank_delta),
                    safe_str(movement), safe_str(track_name), safe_str(artists), safe_str(release_date)
                )
            try:
                window.status.showMessage(f"Loaded {len(rows)} Nhaccuatui rows (fallback).")
            except Exception:
                pass
        except Exception as e:
            print("Error while loading spotify (fallback):", e)


def main():
    parser = argparse.ArgumentParser(description="Launch BigData GUI and load MySQL tables")
    parser.add_argument("--articles-limit", type=int, default=100, help="Number of article rows to load")
    parser.add_argument("--spotify-limit", type=int, default=200, help="Number of spotify rows to load")
    args = parser.parse_args()

    app = QApplication(sys.argv)

    window = BigDataGUI()
    window.show()

    # If GUI provides dedicated loader methods, prefer them.
    try:
        if hasattr(window, "load_articles_from_db"):
            try:
                window.load_articles_from_db(limit=args.articles_limit)
            except Exception as e:
                print("Warning: load_articles_from_db raised:", e)
        if hasattr(window, "load_spotify_from_db"):
            try:
                window.load_spotify_from_db(limit=args.spotify_limit)
            except Exception as e:
                print("Warning: load_spotify_from_db raised:", e)

        need_fallback = False
        try:
            if window.vnexpress_table.rowCount() == 0 or window.nhaccuatui_table.rowCount() == 0:
                need_fallback = True
        except Exception:
            need_fallback = True

        if need_fallback:
            fill_from_fetch_funcs(window, args.articles_limit, args.spotify_limit)

    except Exception as e:
        print("Unexpected error while attempting to load data:", e)
        try:
            window.status.showMessage(f"Error loading data: {e}")
        except Exception:
            pass

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
