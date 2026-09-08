/* ==========================================================================
   SACI Documentation: behaviour
   No framework. Every block here is a small, independent enhancement; the
   site is readable and navigable with JavaScript disabled.
   ========================================================================== */

(() => {
  'use strict';

  const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  /** Normalise a pathname for comparison: strip trailing slash and index.html. */
  function normalisePath(pathname) {
    return pathname.replace(/index\.html$/, '').replace(/\/+$/, '') || '/';
  }

  /** Same-origin check that survives absolute internal URLs. */
  function isExternal(anchor) {
    // `anchor.href` is always absolute and already resolved by the browser.
    return anchor.origin !== window.location.origin;
  }

  function escapeHtml(text) {
    return text.replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  }

  // --------------------------------------------------------------------------
  // Zola tags markdown code fences as `<code data-lang="go">`, while Prism only
  // looks at `class="language-*"`. Without this bridge, the only highlighted
  // blocks are the hand-written ones in templates/*.html, which carry the class
  // themselves.
  //
  // Zola reports the name of the syntax it resolved the fence to, not the token
  // the page wrote, so a ```bash fence arrives as `shellscript`. Prism knows
  // that grammar as `bash`, so the two vocabularies need one small table.
  //
  // This runs at deferred-script time, before Prism's own DOMContentLoaded
  // pass, so Prism sees the class on its first and only sweep.
  // --------------------------------------------------------------------------
  const prismIds = { shellscript: 'bash' };

  document.querySelectorAll('pre > code[data-lang]').forEach((code) => {
    const lang = prismIds[code.dataset.lang] || code.dataset.lang;
    if (lang && !code.classList.contains(`language-${lang}`)) {
      code.classList.add(`language-${lang}`);
    }
  });

  // --------------------------------------------------------------------------
  // Prism has no KDL component, so config fences would sit unhighlighted next
  // to Rust and TOML. This grammar covers what the pages write: node names,
  // property names, quoted and raw strings, `#`-prefixed literals, numbers and
  // comments.
  //
  // Same window as the bridge above: the Prism scripts are deferred ahead of
  // this one, so the language is registered before Prism's only sweep.
  // --------------------------------------------------------------------------
  if (window.Prism) {
    const ident = /[^\s(){}\\/=";]+/.source;
    window.Prism.languages.kdl = {
      comment: [
        { pattern: /\/\*[\s\S]*?(?:\*\/|$)/, greedy: true },
        { pattern: /\/\/.*/, greedy: true },
      ],
      string: {
        pattern: /"""[\s\S]*?"""|#+"[\s\S]*?"#+|"(?:\\[\s\S]|[^\\"])*"/,
        greedy: true,
      },
      // Prism sees no line structure, so order carries the meaning: a name
      // before `=` is a property, a `#` literal or a bare number is a value,
      // and the word left over is a node name. Values on these pages are
      // always quoted, so nothing else reaches the last rule.
      'attr-name': RegExp(`${ident}(?==)`),
      boolean: /#(?:true|false)\b/,
      keyword: /#(?:null|inf|-inf|nan)\b/,
      number: /[+-]?(?:0[xX][\da-fA-F_]+|0[oO][0-7_]+|0[bB][01_]+|\d[\d_]*(?:\.\d[\d_]*)?(?:[eE][+-]?\d+)?)/,
      function: RegExp(ident),
      operator: /=/,
      punctuation: /[{};]/,
    };
  }

  document.addEventListener('DOMContentLoaded', () => {
    const root = document.documentElement;
    const sidebar = document.querySelector('.sidebar');
    const navToggle = document.getElementById('nav-toggle');
    const overlay = document.querySelector('.sidebar-overlay');
    const content = document.getElementById('content');

    // ------------------------------------------------------------------------
    // Theme
    //
    // The stylesheet resolves both themes through `light-dark()`, so switching
    // is one attribute. It is only written once the reader picks a side; until
    // then the operating system decides.
    // ------------------------------------------------------------------------
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
      const systemDark = window.matchMedia('(prefers-color-scheme: dark)');
      const current = () => root.dataset.theme || (systemDark.matches ? 'dark' : 'light');
      const label = () => {
        themeToggle.setAttribute('aria-label', current() === 'dark' ? 'Switch to light theme' : 'Switch to dark theme');
      };
      label();
      themeToggle.addEventListener('click', () => {
        const next = current() === 'dark' ? 'light' : 'dark';
        root.dataset.theme = next;
        try { localStorage.setItem('saci:theme', next); } catch (e) {}
        label();
      });
      systemDark.addEventListener('change', label);
    }

    // ------------------------------------------------------------------------
    // External links open in a new tab. Internal ones must not.
    //
    // Zola's `get_url` emits absolute URLs because `base_url` is absolute, so
    // a naive `a[href^="http"]` selector matches every internal nav link and
    // sends the whole site to new tabs. Compare origins instead.
    // ------------------------------------------------------------------------
    document.querySelectorAll('a[href]').forEach((link) => {
      if (link.hasAttribute('target')) return;
      if (!isExternal(link)) return;
      link.setAttribute('target', '_blank');
      link.setAttribute('rel', 'noopener noreferrer');
      // Image-only links (e.g. the CI badge) get no ↗ mark: the glyph wraps
      // below the image and knocks it out of line with its neighbours.
      if (!link.querySelector('img')) link.classList.add('is-external');
    });

    // ------------------------------------------------------------------------
    // Active nav link
    //
    // Compare normalised pathnames, not raw href strings: the hrefs are
    // absolute and the current URL may or may not carry a trailing slash.
    // ------------------------------------------------------------------------
    const here = normalisePath(window.location.pathname);
    let activeLink = null;

    document.querySelectorAll('.nav-links a').forEach((link) => {
      if (isExternal(link)) return;
      if (normalisePath(link.pathname) === here) {
        link.classList.add('active');
        link.setAttribute('aria-current', 'page');
        activeLink = link;
      }
    });

    // The sidebar links to leaf pages, so a section landing page like
    // /benchmarks/ matches nothing above and the nav goes blank. Fall back to
    // the first link that lives under the current directory, and mark it as an
    // ancestor rather than the page itself.
    if (!activeLink && here !== '/') {
      const prefix = here.endsWith('/') ? here : here + '/';
      for (const link of document.querySelectorAll('.nav-links a')) {
        if (isExternal(link)) continue;
        if (normalisePath(link.pathname).startsWith(prefix)) {
          link.classList.add('active');
          link.setAttribute('aria-current', 'true');
          activeLink = link;
          break;
        }
      }
    }

    // A child page highlights itself; its parent entry is the trail to it, not
    // the page you are on, so it gets a weaker marker.
    if (activeLink) {
      const sub = activeLink.closest('.nav-sublinks');
      if (sub && sub.parentElement) {
        const parentLink = sub.parentElement.querySelector(':scope > a');
        if (parentLink) parentLink.classList.add('trail');
      }
    }

    // Collapsible nav groups. A stored choice wins; without one, the group
    // holding the active link stays open and the rest collapse. The server
    // HTML ships fully expanded, so with scripting off every group is visible.
    // The key carries the area, because both areas name a group "Getting
    // started" and a choice in one must not fold the other's.
    const GROUPS_KEY = 'saci:nav-groups';
    let savedGroups = {};
    try { savedGroups = JSON.parse(localStorage.getItem(GROUPS_KEY)) || {}; } catch (e) {}
    document.querySelectorAll('.nav-group').forEach((group) => {
      const label = group.querySelector('.nav-group-label');
      if (!label) return;
      const name = document.body.dataset.area + ':' + label.querySelector('span').textContent.trim();
      const isOpen = savedGroups[name] ?? Boolean(group.querySelector('.nav-links a.active'));
      group.classList.toggle('collapsed', !isOpen);
      label.setAttribute('aria-expanded', String(isOpen));
      label.addEventListener('click', () => {
        const nowOpen = group.classList.toggle('collapsed') === false;
        label.setAttribute('aria-expanded', String(nowOpen));
        savedGroups[name] = nowOpen;
        try { localStorage.setItem(GROUPS_KEY, JSON.stringify(savedGroups)); } catch (e) {}
      });
    });

    // ------------------------------------------------------------------------
    // Keep the sidebar's scroll position across navigations.
    //
    // The site is server-rendered, so every click is a real page load. Without
    // this the nav jumps back to the top each time.
    // ------------------------------------------------------------------------
    if (sidebar) {
      const KEY = 'saci:sidebar-scroll';
      try {
        const saved = sessionStorage.getItem(KEY);
        if (saved !== null) sidebar.scrollTop = parseInt(saved, 10) || 0;
      } catch (e) {}

      // If the active link ended up off-screen, bring it into view instead.
      if (activeLink) {
        const linkBox = activeLink.getBoundingClientRect();
        const navBox = sidebar.getBoundingClientRect();
        if (linkBox.top < navBox.top || linkBox.bottom > navBox.bottom) {
          activeLink.scrollIntoView({ block: 'center' });
        }
      }

      window.addEventListener('beforeunload', () => {
        try { sessionStorage.setItem(KEY, String(sidebar.scrollTop)); } catch (e) {}
      });
    }

    // Prefetch internal pages on hover or focus, before the click.
    if (!navigator.connection || !navigator.connection.saveData) {
      const prefetched = new Set([normalisePath(window.location.pathname)]);

      const prefetch = (href) => {
        const path = normalisePath(new URL(href).pathname);
        if (prefetched.has(path)) return;
        prefetched.add(path);
        const hint = document.createElement('link');
        hint.rel = 'prefetch';
        hint.href = href;
        document.head.appendChild(hint);
      };

      document.querySelectorAll('.sidebar a, .next-card, .pager-link').forEach((link) => {
        if (!link.href || isExternal(link)) return;
        link.addEventListener('pointerenter', () => prefetch(link.href), { once: true });
        link.addEventListener('focus', () => prefetch(link.href), { once: true });
      });
    }

    // Mobile navigation drawer.
    function setDrawer(open) {
      if (!sidebar) return;
      sidebar.classList.toggle('open', open);
      if (overlay) overlay.classList.toggle('visible', open);
      if (navToggle) navToggle.setAttribute('aria-expanded', String(open));
      document.body.classList.toggle('nav-open', open);
    }

    if (navToggle) {
      navToggle.addEventListener('click', (e) => {
        e.stopPropagation();
        setDrawer(!sidebar.classList.contains('open'));
      });
    }

    if (overlay) overlay.addEventListener('click', () => setDrawer(false));

    // Following a link inside the drawer should close it.
    if (sidebar) {
      sidebar.addEventListener('click', (e) => {
        if (e.target.closest('a')) setDrawer(false);
      });
    }

    // ------------------------------------------------------------------------
    // On this page
    //
    // Built from the headings the page actually rendered, so template pages and
    // markdown pages get the same rail without either declaring it twice. Wide
    // viewports pin it open beside the prose; narrow ones fold it into a
    // disclosure above the content.
    // ------------------------------------------------------------------------
    const tocHost = document.getElementById('page-toc');
    const headings = content ? Array.from(content.querySelectorAll('h2[id]')) : [];

    if (tocHost && headings.length > 1) {
      const details = document.createElement('details');
      details.className = 'toc-inline';

      const summary = document.createElement('summary');
      summary.textContent = 'On this page';
      details.appendChild(summary);

      const title = document.createElement('div');
      title.className = 'toc-title';
      title.textContent = 'On this page';
      details.appendChild(title);

      const list = document.createElement('ul');
      headings.forEach((heading) => {
        const li = document.createElement('li');
        const link = document.createElement('a');
        link.href = `#${encodeURIComponent(heading.id)}`;
        link.textContent = heading.textContent.trim();
        li.appendChild(link);
        list.appendChild(li);
      });
      details.appendChild(list);

      const top = document.createElement('a');
      top.className = 'toc-top';
      top.href = '#';
      top.textContent = 'Back to top';
      top.addEventListener('click', (e) => {
        e.preventDefault();
        window.scrollTo({ top: 0, behavior: reduceMotion ? 'auto' : 'smooth' });
      });
      details.appendChild(top);

      // Two placements, one node. On the rail it is pinned open beside the
      // prose; once the rail is gone it becomes a disclosure ahead of the
      // first section, where it reads as part of the page rather than a
      // header above the title.
      const wide = window.matchMedia('(min-width: 1280px)');
      const place = () => {
        details.open = wide.matches;
        details.classList.toggle('toc-card', !wide.matches);
        if (wide.matches) {
          tocHost.appendChild(details);
          tocHost.hidden = false;
        } else {
          tocHost.hidden = true;
          content.insertBefore(details, headings[0]);
        }
      };
      place();
      wide.addEventListener('change', place);

      // Highlight the entry for the section in view.
      const tocLinks = Array.from(list.querySelectorAll('a'));
      if ('IntersectionObserver' in window) {
        const byTarget = new Map();
        tocLinks.forEach((link, i) => byTarget.set(headings[i], link));

        const spy = new IntersectionObserver(
          (entries) => {
            entries.forEach((entry) => {
              const link = byTarget.get(entry.target);
              if (!link || !entry.isIntersecting) return;
              tocLinks.forEach((l) => l.classList.remove('reading'));
              link.classList.add('reading');
            });
          },
          { rootMargin: '-96px 0px -70% 0px' }
        );

        headings.forEach((heading) => spy.observe(heading));
      }
    }

    // ------------------------------------------------------------------------
    // Search
    //
    // The index is a flat list of page sections, built after the site is
    // rendered so template-authored pages are covered too. It is fetched on
    // first use, not on page load.
    // ------------------------------------------------------------------------
    const dialog = document.getElementById('search-dialog');
    const searchOpen = document.getElementById('search-open');

    if (dialog && searchOpen && typeof dialog.showModal === 'function') {
      const input = document.getElementById('search-input');
      const results = document.getElementById('search-results');
      const closeBtn = dialog.querySelector('.cmdk-close');
      const indexUrl = new URL(dialog.dataset.index, window.location.href);
      const fileMode = window.location.protocol === 'file:';

      let records = null;
      let loading = null;
      let items = [];
      let cursor = -1;

      function loadIndex() {
        if (records) return Promise.resolve(records);
        if (!loading) {
          loading = fetch(indexUrl)
            .then((r) => (r.ok ? r.json() : []))
            .then((data) => { records = data; return records; })
            .catch(() => { records = []; return records; });
        }
        return loading;
      }

      // A record's `a` field is the area it belongs to; the landing page and
      // the 404 carry the empty string, so both drop out of the label.
      const AREA = { service: 'Service', library: 'Library' };

      function score(record, terms) {
        const title = record.t.toLowerCase();
        const section = (record.s || '').toLowerCase();
        const body = record.b.toLowerCase();
        let total = 0;

        for (const term of terms) {
          let best = 0;
          if (title.includes(term)) best = title.startsWith(term) ? 10 : 7;
          if (section.includes(term)) best = Math.max(best, section.startsWith(term) ? 9 : 6);
          if (best === 0) {
            const hits = body.split(term).length - 1;
            if (hits === 0) return 0;
            best = Math.min(hits, 4);
          }
          total += best;
        }
        return total;
      }

      function snippet(record, terms) {
        const body = record.b;
        const lower = body.toLowerCase();
        let at = -1;
        for (const term of terms) {
          const found = lower.indexOf(term);
          if (found !== -1 && (at === -1 || found < at)) at = found;
        }
        const start = at === -1 ? 0 : Math.max(0, at - 50);
        let text = body.slice(start, start + 170).trim();
        if (start > 0) text = '…' + text;
        if (start + 170 < body.length) text += '…';

        let html = escapeHtml(text);
        for (const term of terms) {
          const safe = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          html = html.replace(new RegExp(`(${safe})`, 'gi'), '<mark>$1</mark>');
        }
        return html;
      }

      function resolve(url) {
        const target = new URL(url, indexUrl);
        if (fileMode && target.pathname.endsWith('/')) target.pathname += 'index.html';
        return target.href;
      }

      function select(next) {
        if (items.length === 0) return;
        cursor = (next + items.length) % items.length;
        items.forEach((item, i) => item.setAttribute('aria-selected', String(i === cursor)));
        items[cursor].scrollIntoView({ block: 'nearest' });
      }

      function render(query) {
        const terms = query.toLowerCase().split(/\s+/).filter(Boolean);
        results.innerHTML = '';
        items = [];
        cursor = -1;
        input.setAttribute('aria-expanded', String(terms.length > 0));

        if (terms.length === 0) {
          results.innerHTML = '<p class="cmdk-empty">Type to search every page, section by section.</p>';
          return;
        }
        if (!records) {
          results.innerHTML = '<p class="cmdk-empty">Loading the index…</p>';
          return;
        }

        const area = document.body.dataset.area;
        const hits = records
          .map((record) => {
            // A reader in one area wants that area's pages first. The bonus is
            // added to a hit only, so it can never promote a non-match.
            const base = score(record, terms);
            return { record, rank: base > 0 ? base + (record.a === area ? 2 : 0) : 0 };
          })
          .filter((hit) => hit.rank > 0)
          .sort((a, b) => b.rank - a.rank)
          .slice(0, 10);

        if (hits.length === 0) {
          results.innerHTML = `<p class="cmdk-empty">No matches for “${escapeHtml(query)}”.</p>`;
          return;
        }

        let group = null;
        for (const { record } of hits) {
          const heading = [AREA[record.a], record.g === record.t ? '' : record.g, record.t]
            .filter(Boolean)
            .join(' · ');
          if (heading !== group) {
            group = heading;
            const label = document.createElement('div');
            label.className = 'cmdk-group-label';
            label.textContent = heading;
            results.appendChild(label);
          }

          const item = document.createElement('a');
          item.className = 'cmdk-item';
          item.href = resolve(record.u);
          item.setAttribute('role', 'option');
          item.setAttribute('aria-selected', 'false');
          item.innerHTML =
            `<span class="cmdk-item-title">${escapeHtml(record.s || record.t)}</span>` +
            `<span class="cmdk-item-text">${snippet(record, terms)}</span>`;
          item.addEventListener('mouseenter', () => select(items.indexOf(item)));
          results.appendChild(item);
          items.push(item);
        }
        select(0);
      }

      function open() {
        if (dialog.open) return;
        dialog.showModal();
        render(input.value);
        input.focus();
        input.select();
        loadIndex().then(() => { if (dialog.open) render(input.value); });
      }

      // The trigger is rendered with the Windows and Linux spelling.
      const hint = searchOpen.querySelector('.kbd');
      if (hint && /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent)) {
        hint.textContent = '\u2318K';
      }

      searchOpen.addEventListener('click', open);
      if (closeBtn) closeBtn.addEventListener('click', () => dialog.close());

      input.addEventListener('input', () => render(input.value));

      input.addEventListener('keydown', (e) => {
        if (e.key === 'ArrowDown') { e.preventDefault(); select(cursor + 1); }
        else if (e.key === 'ArrowUp') { e.preventDefault(); select(cursor - 1); }
        else if (e.key === 'Enter' && cursor >= 0) { e.preventDefault(); items[cursor].click(); }
      });

      // Clicking the backdrop closes: the dialog element itself fills the
      // viewport for hit-testing, so compare against its own box.
      dialog.addEventListener('click', (e) => {
        const box = dialog.getBoundingClientRect();
        const inside = e.clientX >= box.left && e.clientX <= box.right &&
                       e.clientY >= box.top && e.clientY <= box.bottom;
        if (!inside) dialog.close();
      });

      document.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
          e.preventDefault();
          dialog.open ? dialog.close() : open();
        } else if (e.key === '/' && !dialog.open && !/^(INPUT|TEXTAREA)$/.test(document.activeElement.tagName)) {
          e.preventDefault();
          open();
        }
      });
    }

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') setDrawer(false);
    });

    // ------------------------------------------------------------------------
    // Diagram pan hint
    //
    // A diagram is authored at 660 units and its labels are sized for that, so
    // the frame scrolls rather than scaling the type down. Whether it scrolls
    // depends on the column width, which no media query knows precisely: the
    // sidebar width and padding both change at breakpoints. Measure instead,
    // and only show the hint when it is true.
    // ------------------------------------------------------------------------
    const frames = document.querySelectorAll('.dgm-scroll');
    if (frames.length > 0) {
      const syncHints = () => {
        frames.forEach((frame) => {
          const dgm = frame.closest('.dgm');
          if (!dgm) return;
          dgm.classList.toggle('is-pannable', frame.scrollWidth > frame.clientWidth + 1);
        });
      };
      syncHints();
      if ('ResizeObserver' in window) {
        const ro = new ResizeObserver(syncHints);
        frames.forEach((frame) => ro.observe(frame));
      } else {
        window.addEventListener('resize', syncHints);
      }
    }

    const revealTargets = document.querySelectorAll('.animate-in');
    if (revealTargets.length > 0) {
      if (reduceMotion || !('IntersectionObserver' in window)) {
        revealTargets.forEach((el) => el.classList.add('revealed'));
      } else {
        const reveal = new IntersectionObserver(
          (entries) => {
            entries.forEach((entry) => {
              if (!entry.isIntersecting) return;
              entry.target.classList.add('revealed');
              reveal.unobserve(entry.target);
            });
          },
          { threshold: 0.05, rootMargin: '0px 0px -32px 0px' }
        );
        revealTargets.forEach((el) => reveal.observe(el));
      }
    }

    const backToTop = document.querySelector('.back-to-top');
    if (backToTop) {
      const onScroll = () => {
        backToTop.classList.toggle('visible', window.scrollY > 600);
      };
      onScroll();
      window.addEventListener('scroll', onScroll, { passive: true });
      backToTop.addEventListener('click', () => {
        window.scrollTo({ top: 0, behavior: reduceMotion ? 'auto' : 'smooth' });
      });
    }

    // ------------------------------------------------------------------------
    // A markdown fence renders as a bare `<pre>`, while the concept pages in
    // templates/*.html hand-author the same snippet already wrapped in `.code`
    // with a caption. This pass gives the markdown blocks that frame too: the
    // language name comes from `data-lang`, and the fence's `name=` annotation
    // arrives as `data-name` and becomes the short description beside it.
    //
    // Runs before the copy-button pass below, which then walks the moved
    // `<pre>` elements in their new parents.
    // ------------------------------------------------------------------------
    const codeLabels = {
      rust: 'Rust',
      go: 'Go',
      python: 'Python',
      typescript: 'TypeScript',
      ts: 'TypeScript',
      kotlin: 'Kotlin',
      csharp: 'C#',
      cs: 'C#',
      bash: 'Bash',
      shellscript: 'Bash',
      json: 'JSON',
      xml: 'XML',
      toml: 'TOML',
      sql: 'SQL',
      kdl: 'KDL',
      wit: 'WIT',
      text: 'Text',
      plain: 'Text',
    };

    /** Human label for a fence language: the table above, else the token itself. */
    function codeLabel(lang) {
      if (!lang) return 'Code';
      return codeLabels[lang] || lang.charAt(0).toUpperCase() + lang.slice(1);
    }

    document.querySelectorAll('pre').forEach((pre) => {
      const code = pre.querySelector('code');
      const parent = pre.parentElement;
      // No `<code>` is not a snippet; a `.code` parent is already framed.
      if (!code || !parent || parent.classList.contains('code')) return;

      const caption = document.createElement('div');
      caption.className = 'code-cap';

      const label = document.createElement('span');
      label.textContent = codeLabel(code.dataset.lang);
      caption.appendChild(label);

      if (code.dataset.name) {
        const description = document.createElement('em');
        description.textContent = code.dataset.name;
        caption.appendChild(description);
      }

      const frame = document.createElement('div');
      frame.className = 'code';
      parent.insertBefore(frame, pre);
      frame.appendChild(caption);
      frame.appendChild(pre);
    });

    document.querySelectorAll('pre').forEach((pre) => {
      const code = pre.querySelector('code');
      if (!code) return;

      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'copy-btn';
      button.textContent = 'Copy';
      button.setAttribute('aria-label', 'Copy code to clipboard');

      button.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(code.innerText);
          button.textContent = 'Copied';
          button.classList.add('copied');
        } catch {
          button.textContent = 'Press Ctrl+C';
        }
        setTimeout(() => {
          button.textContent = 'Copy';
          button.classList.remove('copied');
        }, 1800);
      });

      pre.appendChild(button);
    });
  });
})();
