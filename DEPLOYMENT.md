# Streamlit Cloud Deployment Guide

This guide will help you deploy your Noise Monitoring app to Streamlit Cloud so it's accessible live (not on localhost).

## Prerequisites

✅ GitHub repository (already set up)  
✅ Supabase database (already set up)  
✅ Streamlit Cloud account (free tier available)

## Step 1: Create Streamlit Cloud Account

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Authorize Streamlit Cloud to access your repositories

## Step 2: Deploy Your App

1. In Streamlit Cloud dashboard, click **"New app"**
2. Select your repository: `noise-monitoring-project`
3. Select the branch: `main` (or your default branch)
4. Set the **Main file path**: `streamlit_app.py`
5. Click **"Deploy"**

## Step 3: Configure Environment Variables

After deployment starts, you need to add your environment variables:

1. In your app's settings (click the "⋮" menu → "Settings")
2. Go to **"Secrets"** section
3. Click **"Edit secrets"** and add the following secrets in TOML format:

```toml
SUPABASE_URL = "https://tgznxzfdlxhxqwpohhyl.supabase.co"
SUPABASE_ANON_KEY = "sb_secret_W_W0nOIKg-2wLhjxmZiKFA_oMxZq8jk"
APP_USERNAME = "your_preferred_username"
APP_PASSWORD = "your_secure_password"
SUPABASE_WIDE_VIEW = "wide_view"
```

**Important:**
- Use the exact Supabase URL and anon key provided
- Set `APP_USERNAME` and `APP_PASSWORD` to your desired login credentials (keep these secure!)
- The `SUPABASE_WIDE_VIEW` should match your view name in Supabase (default: `wide_view`)
- Make sure to use TOML format (with quotes around string values)

## Step 4: Verify Database Setup

Before the app works, ensure your Supabase database has:

1. **The `meter_readings` table** (if not already created):
   ```sql
   CREATE TABLE IF NOT EXISTS public.meter_readings (
     location_id text NOT NULL,
     location_name text,
     reading_value double precision,
     reading_datetime timestamptz NOT NULL,
     created_at timestamptz NOT NULL DEFAULT now(),
     CONSTRAINT meter_readings_pkey PRIMARY KEY (location_id, reading_datetime)
   );
   
   CREATE INDEX IF NOT EXISTS idx_meter_readings_datetime
   ON public.meter_readings (reading_datetime);
   ```

2. **The `wide_view` view** (or materialized view):
   - This view should pivot the data with Date, Time, and one column per location_id
   - If you need help creating this view, check your Supabase SQL editor

3. **Data loaded** into the table (run your ETL scripts if needed)

## Step 5: Access Your Live App

Once deployed:
- Your app will be available at: `https://your-app-name.streamlit.app`
- The URL will be shown in your Streamlit Cloud dashboard
- You can share this URL to present on your laptop

## Troubleshooting

### App shows "Database Not Set Up" error
- Verify your `SUPABASE_URL` and `SUPABASE_ANON_KEY` are correct
- Check that the `wide_view` exists in your Supabase database
- Ensure your Supabase project allows connections from Streamlit Cloud

### Login not working
- Verify `APP_USERNAME` and `APP_PASSWORD` are set correctly in secrets
- Check for any typos in the environment variables

### No data showing
- Ensure data has been loaded into the `meter_readings` table
- Verify the `wide_view` is correctly created and contains data
- Check the date range filter - try selecting a wider range

### Deployment fails
- Check the deployment logs in Streamlit Cloud
- Verify `requirements.txt` has all necessary packages
- Ensure `streamlit_app.py` exists in the root directory

## Features of Your Deployed App

✅ **Secure Login**: Username/password authentication  
✅ **Interactive Filters**: Date range, location selection, value range  
✅ **Data Visualization**: Formatted table with statistics  
✅ **Pagination**: Navigate through large datasets  
✅ **Export**: Download filtered data as CSV  
✅ **Real-time Data**: Connected to your live Supabase database

## Cost

Streamlit Cloud offers a **free tier** that includes:
- Unlimited public apps
- 1 private app
- Sufficient resources for this monitoring app

## Support

If you encounter issues:
1. Check the Streamlit Cloud logs
2. Verify your Supabase connection
3. Test locally first: `streamlit run streamlit_app.py`

---

**Your app is now live and ready to present! 🎉**

