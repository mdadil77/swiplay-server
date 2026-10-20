# swiplay-server
# Swiplay Gaming Lounge - Deployment Guide

Welcome to the live, real-time version of Swiplay Gaming Lounge! This setup uses **GitHub Pages** to host your frontend files for free, and **Render.com** to host your Node.js multiplayer server for free.

## Step 1: Deploy the Server (Render.com)

1. Sign up for a free account at [Render.com](https://render.com).
2. Go to your Dashboard and click **New +** > **Web Service**.
3. Choose **Build and deploy from a Git repository**.
4. Connect the GitHub repository where you saved your `server/` folder.
5. In the settings:
   - **Name**: `swiplay-server`
   - **Environment**: Node
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
   - **Plan**: Free
6. Scroll down to **Environment Variables** and add:
   - `ALLOWED_ORIGINS`: `*` (You can change this later to your GitHub Pages URL for better security, e.g., `https://yourname.github.io`).
7. Click **Create Web Service**. 
8. Render will take a few minutes to build. Once done, copy the URL at the top left (it will look like `https://swiplay-server-xxxxx.onrender.com`).

## Step 2: Update the Frontend
1. Open your `index.html` file on your computer.
2. Find line 39: `const SERVER_URL = 'https://YOUR-SERVER.onrender.com';`
3. Replace that URL with the Render URL you just copied.
4. Do the exact same thing in `swipe-game.html` on line 11.
5. Commit and push these updated HTML files to your GitHub repository so GitHub Pages updates them.

## Step 3: Test It Out! (Checklist)
Open your GitHub Pages URL in two different browser windows side-by-side (use Incognito mode for the second window so it forces you to create a new profile).

- [ ] **Global Chat**: Type a message in window A. Does it instantly show up in window B?
- [ ] **Profanity Filter**: Try typing a bad word. Does it change to `***`?
- [ ] **Matchmaking**: Click "Swipe Game" > "Play with Random" in window A. Then do the same in window B. They should match and teleport to the game screen together!
- [ ] **Game Sync**: Watch the 3-2-1 countdown. It should happen at the exact same time in both windows.
- [ ] **Live Score**: Swipe the correct arrow in window A. Watch window B—does window A's score go up instantly on window B's screen?
- [ ] **Friend Invite**: Go back to the lobby. Find the other player in the "Active Players" list and click "Invite". Accept the invite in the other window.

**Note on Service Workers (`sw.js`)**: The provided `sw.js` safely caches your HTML/CSS but naturally ignores WebSocket (`wss://`) traffic. It is perfectly safe to use alongside Socket.io.

**Note on the Free Tier**: Render's free servers "go to sleep" if nobody uses them for 15 minutes. If this happens, the next time someone opens your website, it will take about 30 seconds for the server to wake up. The frontend code is already built to show a friendly "Waking up server..." message while this happens!
