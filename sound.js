const BednaSound = (() => {
  const STORAGE_KEY = "bedna_sound_enabled";
  const AudioCtx = window.AudioContext || window.webkitAudioContext;
  let context = null;
  let enabled = true;
  let unlockBound = false;

  const patterns = {
    click: [[740, 0.04, "triangle", 0.03]],
    success: [[620, 0.08, "triangle", 0.04], [820, 0.11, "triangle", 0.04]],
    error: [[360, 0.1, "sawtooth", 0.04], [250, 0.12, "sawtooth", 0.04]],
    notify: [[880, 0.06, "sine", 0.03], [1100, 0.08, "sine", 0.03]],
    join: [[540, 0.06, "triangle", 0.035], [700, 0.09, "triangle", 0.035]],
    leave: [[660, 0.06, "triangle", 0.03], [460, 0.09, "triangle", 0.03]],
    approve: [[560, 0.06, "triangle", 0.035], [740, 0.07, "triangle", 0.035], [920, 0.09, "triangle", 0.035]],
    reject: [[470, 0.08, "sawtooth", 0.035], [390, 0.09, "sawtooth", 0.035]],
    send: [[780, 0.05, "sine", 0.03]],
    message: [[930, 0.04, "triangle", 0.028]],
    ban: [[300, 0.1, "sawtooth", 0.04], [220, 0.14, "sawtooth", 0.04]],
    modal: [[660, 0.05, "sine", 0.03], [840, 0.07, "sine", 0.03]]
  };

  function ensureContext() {
    if (!enabled || !AudioCtx) {
      return null;
    }
    if (!context) {
      context = new AudioCtx();
    }
    return context;
  }

  function bindUnlock() {
    if (unlockBound) {
      return;
    }
    unlockBound = true;
    const unlock = () => {
      if (!enabled || !context) {
        return;
      }
      if (context.state === "suspended") {
        context.resume().catch(() => {});
      }
    };
    ["pointerdown", "keydown", "touchstart"].forEach((eventName) => {
      window.addEventListener(eventName, unlock, { passive: true });
    });
  }

  function playTone(ctx, startAt, tone) {
    const [freq, duration, wave, volume] = tone;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = wave || "sine";
    osc.frequency.setValueAtTime(Number(freq) || 440, startAt);
    gain.gain.setValueAtTime(0.0001, startAt);
    gain.gain.exponentialRampToValueAtTime(Number(volume) || 0.03, startAt + 0.01);
    gain.gain.exponentialRampToValueAtTime(0.0001, startAt + Math.max(0.03, Number(duration) || 0.08));
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(startAt);
    osc.stop(startAt + Math.max(0.04, Number(duration) || 0.08) + 0.02);
  }

  function play(name) {
    if (!enabled) {
      return;
    }
    const ctx = ensureContext();
    if (!ctx) {
      return;
    }
    if (ctx.state === "suspended") {
      ctx.resume().catch(() => {});
    }
    const seq = patterns[name] || patterns.notify;
    let cursor = ctx.currentTime;
    seq.forEach((tone) => {
      playTone(ctx, cursor, tone);
      cursor += Math.max(0.03, Number(tone[1]) || 0.08) * 0.75;
    });
  }

  function setEnabled(next) {
    enabled = Boolean(next);
    try {
      localStorage.setItem(STORAGE_KEY, enabled ? "1" : "0");
    } catch (_error) {
      // Ignore storage failures (private mode, blocked storage, etc.).
    }
    if (enabled) {
      ensureContext();
      bindUnlock();
    }
    return enabled;
  }

  function toggle() {
    return setEnabled(!enabled);
  }

  function isEnabled() {
    return enabled;
  }

  try {
    enabled = localStorage.getItem(STORAGE_KEY) !== "0";
  } catch (_error) {
    enabled = true;
  }
  if (enabled) {
    ensureContext();
    bindUnlock();
  }

  return {
    play,
    toggle,
    setEnabled,
    isEnabled
  };
})();

window.BednaSound = BednaSound;
