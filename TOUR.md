
# MailTrace — Click‑By‑Click Demo (No Tech Skills Needed)

This walkthrough shows you exactly what to click to experience the app locally using demo mode (login disabled).

## 0) One‑time setup
- Install **Python 3.10+** from python.org.
- Open **Terminal** (Mac) or **Command Prompt** (Windows).

## 1) Unzip the file
Unzip `Mailtrace_CLEAN_server_rendered_dashboard.zip`. Inside you will see a folder `Mailtrace/`.

## 2) Start the app (demo mode)
**Windows**: double‑click `run_local.bat` inside the `Mailtrace` folder.  
**Mac**: open Terminal, drag `run_local.sh` into the window, press **Enter**.

You should see: `* Running on http://127.0.0.1:5000/`

## 3) Open the app
Open a browser and go to **http://127.0.0.1:5000**. You’ll land on the dashboard.

## 4) Upload your CSVs
- Click the **Upload** link (top nav) or go to **http://127.0.0.1:5000/runs/upload**.
- Select: your **Direct Mail CSV** and your **Completed Jobs CSV**.
  - Not ready? Use the samples in `app/static/samples/`.
- Click **Upload & Match**.

## 5) Watch it work
- You’ll be redirected back to the **Dashboard** automatically.
- The page will say “Processing…” then the charts and KPIs will fill in.
- What you’ll see:
  - **KPIs**: Total Mail, Matches, Match Rate, Avg. Confidence.
  - **Charts**: Matches by Month, Top Cities/Zips.
  - **Table**: Row‑by‑row matches with confidence badges.

## 6) Explore
- Use the filters at the top to change date ranges.
- Click into a city/zip to see filtered rows.
- Refresh to view your **last uploaded run** again.

## Troubleshooting
- The window says “Can’t open port 5000” → close any other apps using port 5000, then run again.
- The page asks you to log in → demo mode might be off. Close the server and start it with the provided script (it sets `DISABLE_AUTH=1` automatically).
- Blank charts → check your CSV headers match the samples. Even if they don’t, the matcher tries to align common variations.

## Done!
That’s the whole flow a customer will experience — from upload to seeing analytics.
