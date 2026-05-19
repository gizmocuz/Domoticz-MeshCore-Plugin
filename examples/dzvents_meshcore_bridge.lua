--[[
  MeshCore dzVents Command Bridge — example automation script
  ===========================================================

  SETUP
  -----
  1. In Domoticz, open Setup > Hardware and find your MeshCore device.
  2. Edit it and set "Command Bridge Channel" (Mode3) to the channel name you
     want to listen on (e.g. "alerts", "commands", "#nl").
     Leaving this field blank disables the bridge entirely.
  3. Click Save and restart the plugin (disable then enable the hardware).
  4. Three new devices will appear under Setup > Devices:
       - "MeshCore Command In"  (Text)   — receives incoming "!" commands as JSON
       - "MeshCore Reply"       (Text)   — write your reply JSON here first
       - "MeshCore Send"        (Switch) — turn On to fire the reply

  HOW TO TEST
  -----------
  From another MeshCore node (phone app, CLI, or a second node), send a
  message on the configured channel whose text starts with "!":

      !ping
      !status
      !hello world

  Within a few seconds the dzVents script fires and broadcasts the reply back
  to that same channel.  You should see the reply on all nodes monitoring that
  channel.

  ADDING YOUR OWN COMMANDS
  ------------------------
  Extend the if/elseif chain below.  The command string is the full trimmed
  text of the channel message (e.g. "!lights on").  The reply text can be
  anything you compose in Lua — query other Domoticz devices, format sensor
  readings, etc.

  NOTES
  -----
  - The bridge only fires on messages received on the CONFIGURED CHANNEL whose
    text starts with "!".  Private DMs are not intercepted.
  - The channel name comparison is case-insensitive and strips a leading "#",
    so "Alerts", "alerts", and "#alerts" all match a stored value of "alerts".
  - The "id" field in the reply JSON is mandatory when no "to" override is
    used; it lets the plugin look up the configured channel from its internal
    origin table (valid for 5 minutes after the command arrives) and broadcast
    the reply as "#<channel>: <text>".
  - To reply to a different target (or when you have already consumed the id),
    add a "to" key: { id = m.id, to = "OtherNode", text = reply }.
    For a channel broadcast use: { to = "#ChannelName", text = reply }.
  - The payload also carries a "channel" field with the configured channel
    name so scripts can inspect or log it.
]]

return {
    on = {
        devices = { "MeshCore Command In" },
    },

    logging = {
        level  = domoticz.LOG_DEBUG,
        marker = "MeshCore-Bridge",
    },

    execute = function(dz, item)
        -- Parse the incoming command payload.
        local ok, m = pcall(dz.utils.fromJSON, item.text)
        if not ok or type(m) ~= "table" then
            dz.log("Could not parse MeshCore Command In payload: " .. tostring(item.text), dz.LOG_ERROR)
            return
        end

        local cmd     = m.cmd     or ""
        local sender  = m.sender  or "?"
        local channel = m.channel or "?"
        dz.log(string.format("Command from %s on channel %s: %s (id=%s)",
               sender, channel, cmd, tostring(m.id)), dz.LOG_DEBUG)

        -- Command dispatch.
        local reply
        local lower_cmd = string.lower(cmd)

        if lower_cmd == "!ping" then
            reply = "pong"

        elseif lower_cmd == "!status" then
            local ts   = os.date("%Y-%m-%d %H:%M:%S")
            local uptime_min = math.floor(dz.utils.osSeconds() / 60)
            reply = string.format("OK | time=%s uptime=%dm", ts, uptime_min)

        elseif string.sub(lower_cmd, 1, 6) == "!hello" then
            local name = string.sub(cmd, 8)  -- everything after "!hello "
            if name == "" then name = sender end
            reply = "Hello, " .. name .. "!"

        else
            reply = "unknown command: " .. cmd
        end

        -- Build the reply payload and trigger the send.
        -- The plugin will broadcast "#<channel>: <reply>" to the configured channel.
        -- Step 1: write the reply JSON to "MeshCore Reply".
        dz.devices("MeshCore Reply").updateText(
            dz.utils.toJSON({ id = m.id, text = reply })
        )

        -- Step 2: turn on "MeshCore Send" so onCommand fires in the plugin.
        dz.devices("MeshCore Send").switchOn().checkFirst()
    end,
}
