"""
Script: splice new missing-products section into app.py
Replaces lines 2314-3047 (0-indexed 2313 to 3047 inclusive).
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')

APP = 'app.py'
OUT = 'app.py'

with open(APP, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# ── Find section boundaries ─────────────────────────────────────────────────
start = None
end   = None
for i, l in enumerate(lines):
    if 'elif page ==' in l and '\U0001f50d' in l and 'مفقودة' in l:
        start = i
    if start and i > start and 'elif page ==' in l and 'مفقودة' not in l:
        end = i
        break

print(f'Replacing lines {start+1}–{end} ({end - start} lines)')

# ── New section code ─────────────────────────────────────────────────────────
NEW = r'''elif page == "🔍 منتجات مفقودة":
    st.header("🔍 منتجات مفقودة من كتالوجنا")
    db_log("missing", "view")
    # ════════════════════════════ نقطة الدخول ════════════════════════════
    if not (st.session_state.results and "missing" in st.session_state.results):
        st.info("ارفع الملفات أولاً")
    else:
        df = st.session_state.results["missing"]
        if df is None or df.empty:
            st.success("✅ لا توجد منتجات مفقودة في التحليل الحالي!")
        else:
            # ── إحصاءات ──────────────────────────────────────────────────
            total_missing     = len(df)
            confirmed_missing = (
                len(df[df["حالة_المنتج"].str.contains("مفقود مؤكد", na=False)])
                if "حالة_المنتج" in df.columns else total_missing
            )
            potential_dups  = (
                len(df[df["حالة_المنتج"].str.contains("مكرر محتمل", na=False)])
                if "حالة_المنتج" in df.columns else 0
            )
            variants_count  = (
                len(df[df["نوع_متاح"].str.strip() != ""])
                if "نوع_متاح" in df.columns else 0
            )
            _mc1, _mc2, _mc3, _mc4 = st.columns(4)
            _mc1.metric("📦 الإجمالي",       total_missing)
            _mc2.metric("✅ مفقود مؤكد",     confirmed_missing)
            _mc3.metric("⚠️ مكرر محتمل",    potential_dups)
            _mc4.metric("🏷️ نسخة متوفرة",  variants_count)
            if "مستوى_الثقة" in df.columns:
                _gc = len(df[df["مستوى_الثقة"] == "green"])
                _yc = len(df[df["مستوى_الثقة"] == "yellow"])
                _rc = len(df[df["مستوى_الثقة"] == "red"])
                st.markdown(
                    f'<div style="background:#0d1a2e;border-radius:6px;padding:5px 10px;'
                    f'margin:4px 0 8px;font-size:.75rem;color:#aaa">'
                    f'🟢 <b style="color:#4caf50">{_gc}</b> مؤكد &nbsp;'
                    f'🟡 <b style="color:#ff9800">{_yc}</b> محتمل &nbsp;'
                    f'🔴 <b style="color:#f44336">{_rc}</b> مشكوك</div>',
                    unsafe_allow_html=True,
                )

            # ── فلاتر ──────────────────────────────────────────────────────
            opts = get_filter_options(df)
            with st.expander("🔍 فلاتر البحث", expanded=False):
                _fc1, _fc2, _fc3 = st.columns(3)
                _fc4, _fc5, _fc6 = st.columns(3)
                _fc1.text_input("🔎 بحث نصي",   key="miss_s")
                _fc2.selectbox("الماركة",        opts["brands"],      key="miss_b")
                _fc3.selectbox("المنافس",        opts["competitors"], key="miss_c")
                _fc4.selectbox("النوع",          ["الكل", "مفقود فعلاً", "يوجد تستر", "يوجد الأساسي"], key="miss_v")
                _fc5.selectbox("الثقة",          ["الكل", "🟢 مؤكد", "🟡 محتمل", "🔴 مشكوك"],          key="miss_conf_f")
                _fc6.selectbox("حالة المنتج",    ["الكل", "✅ مفقود مؤكد", "⚠️ مكرر محتمل"],          key="miss_gz_f")

            # ── تطبيق الفلاتر ─────────────────────────────────────────────
            _ms_search  = st.session_state.get("miss_s",      "")
            _ms_brand   = st.session_state.get("miss_b",      "الكل")
            _ms_comp    = st.session_state.get("miss_c",      "الكل")
            _ms_variant = st.session_state.get("miss_v",      "الكل")
            _ms_conf    = st.session_state.get("miss_conf_f", "الكل")
            _ms_gz      = st.session_state.get("miss_gz_f",   "الكل")

            filtered = df.copy()
            if _ms_search:
                filtered = filtered[filtered.apply(
                    lambda _r: _ms_search.lower() in str(_r.values).lower(), axis=1)]
            if _ms_brand != "الكل" and "الماركة" in filtered.columns:
                filtered = filtered[filtered["الماركة"].str.contains(
                    _ms_brand, case=False, na=False, regex=False)]
            if _ms_comp != "الكل" and "المنافس" in filtered.columns:
                filtered = filtered[filtered["المنافس"].str.contains(
                    _ms_comp, case=False, na=False, regex=False)]
            if _ms_variant == "مفقود فعلاً" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.strip() == ""]
            elif _ms_variant == "يوجد تستر" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("تستر", na=False)]
            elif _ms_variant == "يوجد الأساسي" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("الأساسي", na=False)]
            if _ms_conf != "الكل" and "مستوى_الثقة" in filtered.columns:
                _cmap = {"🟢 مؤكد": "green", "🟡 محتمل": "yellow", "🔴 مشكوك": "red"}
                _cv   = _cmap.get(_ms_conf, "")
                if _cv:
                    filtered = filtered[filtered["مستوى_الثقة"] == _cv]
            if _ms_gz != "الكل" and "حالة_المنتج" in filtered.columns:
                if _ms_gz == "✅ مفقود مؤكد":
                    filtered = filtered[filtered["حالة_المنتج"].str.startswith("✅", na=False)]
                elif _ms_gz == "⚠️ مكرر محتمل":
                    filtered = filtered[filtered["حالة_المنتج"].str.startswith("⚠️", na=False)]
            if "مستوى_الثقة" in filtered.columns:
                _co = {"green": 0, "yellow": 1, "red": 2}
                filtered = (filtered
                    .assign(_cs=filtered["مستوى_الثقة"].map(_co).fillna(3))
                    .sort_values("_cs").drop(columns=["_cs"]))
            filtered = filtered.reset_index(drop=True)

            # ── session_state للتحديد والصفحة ─────────────────────────────
            if "miss_sel" not in st.session_state:
                st.session_state.miss_sel = set()
            if "miss_pg2" not in st.session_state:
                st.session_state.miss_pg2 = 1

            _MISS_PAGE  = 12
            _total_f    = len(filtered)
            _total_pgs  = max(1, (_total_f + _MISS_PAGE - 1) // _MISS_PAGE)
            _cur_pg     = max(1, min(st.session_state.miss_pg2, _total_pgs))

            # ── شريط التحكم في التحديد ────────────────────────────────────
            _pg_start = (_cur_pg - 1) * _MISS_PAGE
            _pg_end   = min(_pg_start + _MISS_PAGE, _total_f)
            _n_sel    = len(st.session_state.miss_sel)

            _sbc1, _sbc2, _sbc3, _sbc4 = st.columns([2, 2, 2, 4])

            def _cb_sel_page():
                for _pi in range(_pg_start, _pg_end):
                    st.session_state.miss_sel.add(_pi)

            def _cb_sel_all():
                for _pi in range(_total_f):
                    st.session_state.miss_sel.add(_pi)

            def _cb_clr_sel():
                st.session_state.miss_sel.clear()

            _sbc1.button("✓ الصفحة", on_click=_cb_sel_page, key="miss_sel_pg",  use_container_width=True)
            _sbc2.button("✓ الكل",   on_click=_cb_sel_all,  key="miss_sel_all", use_container_width=True)
            _sbc3.button("✗ مسح",    on_click=_cb_clr_sel,  key="miss_clr",     use_container_width=True)
            _sel_c = "#4caf50" if _n_sel > 0 else "#555"
            _sbc4.markdown(
                f'<div style="padding:6px 12px;border-radius:6px;background:#0d1a2e;'
                f'border:1px solid {_sel_c}44;font-size:.83rem;color:{_sel_c};margin-top:2px">'
                f'<b>{_n_sel}</b> محدد من <b>{_total_f}</b> &nbsp;'
                f'<span style="color:#555;font-size:.72rem">صفحة {_cur_pg}/{_total_pgs}</span></div>',
                unsafe_allow_html=True,
            )

            st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)

            # ══════════════════════════════════════════════════════════════
            #  شبكة بطاقات (2 عمود × 6 صف = 12 بطاقة/صفحة)
            # ══════════════════════════════════════════════════════════════
            _page_rows = filtered.iloc[_pg_start:_pg_end]

            for _ri in range(0, len(_page_rows), 2):
                _cols = st.columns(2)
                for _ci, _col in enumerate(_cols):
                    _abs_i = _pg_start + _ri + _ci
                    if _abs_i >= _total_f:
                        break
                    row = _page_rows.iloc[_ri + _ci]
                    idx = _abs_i

                    name  = str(row.get("منتج_المنافس", ""))
                    _miss_key = f"missing_{name}_{idx}"
                    if _miss_key in st.session_state.hidden_products:
                        continue

                    price           = safe_float(row.get("سعر_المنافس", 0))
                    brand           = str(row.get("الماركة",   ""))
                    comp            = str(row.get("المنافس",   ""))
                    size            = str(row.get("الحجم",     ""))
                    ptype           = str(row.get("النوع",     ""))
                    _comp_show      = _humanize_competitor_upload(comp)
                    _title_display  = _display_name_for_missing_row(row)
                    if not _title_display:
                        _u_title = competitor_product_url_from_row(row)
                        if not str(_u_title or "").strip().lower().startswith("http") and _is_http_url_text(name):
                            _u_title = name.strip()
                        if str(_u_title or "").strip().lower().startswith("http"):
                            _ft = _cached_title_from_product_url(str(_u_title).strip())
                            if _ft:
                                _title_display = _ft
                    if _title_display:
                        nm_ai = _title_display
                    elif not _is_http_url_text(name):
                        nm_ai = name
                    else:
                        _fb = f"{brand} {size} {ptype}".strip()
                        nm_ai = _fb if _fb else (_comp_show if _comp_show != "—" else "منتج")

                    note            = str(row.get("ملاحظة", ""))
                    _miss_pid_raw   = (
                        row.get("معرف_المنافس", "") or row.get("product_id", "") or
                        row.get("رقم المنتج",   "") or row.get("رقم_المنتج", "") or
                        row.get("SKU", "")          or row.get("sku", "")         or
                        row.get("الكود", "")        or row.get("كود", "")         or
                        row.get("الباركود", "")     or ""
                    )
                    _miss_pid = ""
                    if _miss_pid_raw and str(_miss_pid_raw) not in ("", "nan", "None", "0", "NaN"):
                        try:    _miss_pid = str(int(float(str(_miss_pid_raw))))
                        except: _miss_pid = str(_miss_pid_raw).strip()

                    variant_label   = str(row.get("نوع_متاح", ""))
                    variant_product = str(row.get("منتج_متاح", ""))
                    variant_score   = safe_float(row.get("نسبة_التشابه", 0))
                    is_tester_flag  = bool(row.get("هو_تستر", False))
                    conf_level      = str(row.get("مستوى_الثقة", "green"))
                    conf_score      = safe_float(row.get("درجة_التشابه", 0))
                    suggested_price = round(price - 1, 2) if price > 0 else 0
                    _gz_status      = str(row.get("حالة_المنتج", "")).strip()
                    _gz_similar     = str(row.get("منتج_مشابه_لدينا", "")).strip()

                    _gray_zone_html = ""
                    if _gz_status.startswith("⚠️"):
                        _sim_t = (f" ← يشبه: <b>{_gz_similar[:50]}</b>" if _gz_similar else "")
                        _gray_zone_html = (
                            f'<div style="margin-top:5px;padding:3px 8px;border-radius:5px;'
                            f'background:rgba(255,152,0,.1);border:1px solid #ff980066;'
                            f'font-size:.7rem;color:#ffb74d;font-weight:700">⚠️ مكرر محتمل{_sim_t}</div>'
                        )
                    elif _gz_status.startswith("✅"):
                        _gray_zone_html = (
                            f'<div style="margin-top:4px;padding:2px 8px;border-radius:5px;'
                            f'background:rgba(0,200,83,.07);border:1px solid #00c85344;'
                            f'font-size:.7rem;color:#69f0ae;font-weight:600">✅ مفقود مؤكد</div>'
                        )

                    _is_similar     = "⚠️" in note
                    _has_variant    = bool(variant_label and variant_label.strip())
                    _is_tester_type = "تستر" in variant_label if _has_variant else False

                    if _has_variant and _is_tester_type:
                        _border = "#ff980055"; _badge_bg = "#ff9800"
                    elif _has_variant:
                        _border = "#4caf5055"; _badge_bg = "#4caf50"
                    elif _is_similar:
                        _border = "#ff572255"; _badge_bg = "#ff5722"
                    else:
                        _border = "#007bff44"; _badge_bg = "#007bff"

                    _variant_html = ""
                    if _has_variant:
                        _variant_html = (
                            f'<div style="margin-top:4px;padding:2px 8px;border-radius:5px;'
                            f'background:{_badge_bg}22;border:1px solid {_badge_bg}66;'
                            f'font-size:.7rem;color:{_badge_bg}">'
                            f'{variant_label} ({variant_score:.0f}%) → {variant_product[:40]}</div>'
                        )
                    _tester_badge = (
                        '<span style="font-size:.65rem;padding:2px 6px;border-radius:8px;'
                        'background:#9c27b022;color:#ce93d8">🏷️ تستر</span>'
                    ) if is_tester_flag else ""

                    _miss_img = str(row.get("صورة_المنافس", "") or "").strip()
                    if not _miss_img:
                        _miss_img = _first_image_url_from_row(row) or ""
                    _miss_comp_url = competitor_product_url_from_row(row)
                    if not _miss_comp_url and _is_http_url_text(name):
                        _miss_comp_url = name.strip()
                    if not _miss_img and _miss_comp_url.startswith("http"):
                        _miss_img = _cached_thumb_from_product_url(_miss_comp_url)

                    _dup_compare_html = ""
                    if _gz_status.startswith("⚠️"):
                        _our_sim_img    = str(row.get("صورة_منتجنا_المشابه", "") or "").strip()
                        _sim_n = (_gz_similar[:45] + "…") if len(_gz_similar) > 45 else _gz_similar
                        _new_n = (str(name)[:45] + "…") if len(str(name)) > 45 else str(name)

                        def _ibox(iu, lbl, sub, ac):
                            if iu and iu.startswith("http"):
                                _t = (f'<img src="{iu}" loading="lazy" decoding="async" '
                                      f'style="width:68px;height:68px;object-fit:cover;border-radius:6px;'
                                      f'border:1px solid {ac}44;display:block;margin:0 auto 3px">')
                            else:
                                _t = (f'<div style="width:68px;height:68px;border-radius:6px;'
                                      f'border:1px dashed {ac}44;display:flex;align-items:center;'
                                      f'justify-content:center;margin:0 auto 3px;font-size:1.3rem;'
                                      f'color:{ac}55">🖼️</div>')
                            return (f'<div style="text-align:center;flex:1;min-width:0">{_t}'
                                    f'<div style="font-size:.62rem;color:{ac};font-weight:700">{lbl}</div>'
                                    f'<div style="font-size:.58rem;color:#666">{sub}</div></div>')

                        _dup_compare_html = (
                            f'<div style="margin-top:7px;padding:6px 8px;border-radius:6px;'
                            f'background:rgba(255,152,0,.05);border:1px solid #ff980020">'
                            f'<div style="font-size:.62rem;color:#777;text-align:center;margin-bottom:4px">'
                            f'قارن الصورتين</div>'
                            f'<div style="display:flex;gap:10px;justify-content:center">'
                            f'{_ibox(_miss_img, "🆕 جديد", _new_n, "#4fc3f7")}'
                            f'<div style="color:#ff980055;font-size:.9rem;align-self:center">⟷</div>'
                            f'{_ibox(_our_sim_img, "📦 لدينا", _sim_n, "#ffb74d")}'
                            f'</div></div>'
                        )

                    with _col:
                        # ── checkbox تحديد ──────────────────────────────────
                        _is_chk = idx in st.session_state.miss_sel

                        def _toggle_sel(i=idx):
                            if i in st.session_state.miss_sel:
                                st.session_state.miss_sel.discard(i)
                            else:
                                st.session_state.miss_sel.add(i)

                        st.checkbox(
                            f"تحديد — {nm_ai[:30]}",
                            value=_is_chk,
                            key=f"mchk_{idx}",
                            on_change=_toggle_sel,
                        )

                        # ── بطاقة المنتج ────────────────────────────────────
                        st.markdown(miss_card(
                            name=name, price=price, brand=brand, size=size,
                            ptype=ptype, comp=_comp_show, suggested_price=suggested_price,
                            note=note if _is_similar else "",
                            variant_html=_variant_html, tester_badge=_tester_badge,
                            border_color=_border,
                            confidence_level=conf_level, confidence_score=conf_score,
                            product_id=_miss_pid,
                            image_url=_miss_img,
                            comp_url=_miss_comp_url,
                            title_override=_title_display,
                            gray_zone_html=_gray_zone_html,
                            dup_compare_html=_dup_compare_html,
                        ), unsafe_allow_html=True)

                        # ── أزرار الإجراءات ─────────────────────────────────
                        _ba1, _ba2, _ba3 = st.columns([3, 3, 2])
                        with _ba1:
                            if st.button("✍️ خبير الوصف", key=f"expert_{idx}",
                                         type="primary", use_container_width=True):
                                with st.spinner("🤖 يكتب الوصف..."):
                                    fi_cached = st.session_state.get(f"frag_info_{idx}")
                                    if not fi_cached:
                                        fi_cached = fetch_fragrantica_info(nm_ai)
                                        st.session_state[f"frag_info_{idx}"] = fi_cached
                                    desc = generate_mahwous_description(nm_ai, suggested_price, fi_cached)
                                    desc, _seo_meta = _parse_seo_json_block(desc)
                                    st.session_state[f"desc_{idx}"] = desc
                        with _ba2:
                            _has_desc = f"desc_{idx}" in st.session_state
                            _lbl = "📤 Make + وصف" if _has_desc else "📤 إرسال Make"
                            if st.button(_lbl, key=f"mk_m_{idx}", use_container_width=True):
                                with st.spinner("📤 يُرسل لـ Make..."):
                                    _res_mk = send_new_products([{
                                        "product_id": _miss_pid, "name": nm_ai,
                                        "price": float(suggested_price), "sku": _miss_pid,
                                        "weight": 1, "cost_price": 0, "sale_price": 0,
                                        "description": st.session_state.get(f"desc_{idx}", f"عطر {nm_ai} الأصلي"),
                                        "image_url": _miss_img,
                                    }])
                                if _res_mk["success"]:
                                    st.success(_res_mk["message"])
                                    st.session_state.hidden_products.add(_miss_key)
                                    st.rerun()
                                else:
                                    st.error(_res_mk["message"])
                        with _ba3:
                            if _miss_comp_url:
                                st.link_button("🔗", _miss_comp_url, use_container_width=True)
                            else:
                                if st.button("🗑", key=f"hide_m_{idx}", use_container_width=True,
                                             help="إخفاء هذا المنتج"):
                                    st.session_state.hidden_products.add(_miss_key)
                                    st.rerun()

                        # ── محرر الوصف (يظهر بعد التوليد) ──────────────────
                        if f"desc_{idx}" in st.session_state:
                            with st.expander("📄 الوصف الكامل", expanded=True):
                                edited_desc = st.text_area(
                                    "راجع وعدّل:",
                                    value=st.session_state[f"desc_{idx}"],
                                    height=200,
                                    key=f"desc_edit_{idx}",
                                )
                                st.session_state[f"desc_{idx}"] = edited_desc
                                _wc = len(edited_desc.split())
                                st.markdown(
                                    f'<span style="color:{"#4caf50" if _wc >= 500 else "#ff9800"};'
                                    f'font-size:.72rem">📊 {_wc} كلمة</span>',
                                    unsafe_allow_html=True,
                                )

            # ── ترقيم الصفحات ─────────────────────────────────────────────
            if _total_pgs > 1:
                st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
                _pg1, _pg2, _pg3, _pg4, _pg5 = st.columns([1, 1, 3, 1, 1])
                if _pg1.button("⏮", key="miss_first", disabled=_cur_pg <= 1):
                    st.session_state.miss_pg2 = 1; st.rerun()
                if _pg2.button("◀", key="miss_prev",  disabled=_cur_pg <= 1):
                    st.session_state.miss_pg2 = _cur_pg - 1; st.rerun()
                _pg3.markdown(
                    f'<div style="text-align:center;padding:5px;font-size:.82rem;color:#aaa">'
                    f'صفحة <b style="color:#fff">{_cur_pg}</b> / <b style="color:#fff">{_total_pgs}</b>'
                    f' <span style="color:#555">({_total_f} منتج)</span></div>',
                    unsafe_allow_html=True,
                )
                if _pg4.button("▶", key="miss_next", disabled=_cur_pg >= _total_pgs):
                    st.session_state.miss_pg2 = _cur_pg + 1; st.rerun()
                if _pg5.button("⏭", key="miss_last", disabled=_cur_pg >= _total_pgs):
                    st.session_state.miss_pg2 = _total_pgs; st.rerun()

            # ══════════════════════════════════════════════════════════════
            #  شريط إجراءات التحديد (يظهر فقط عند وجود تحديد)
            # ══════════════════════════════════════════════════════════════
            _n_sel = len(st.session_state.miss_sel)
            if _n_sel > 0:
                st.markdown('<hr style="border-color:#1e3a5f;margin:14px 0">', unsafe_allow_html=True)
                st.markdown(
                    f'<div style="background:linear-gradient(135deg,#0d2137,#091825);'
                    f'border:1px solid #4caf5044;border-radius:10px;padding:12px 16px;margin-bottom:10px">'
                    f'<span style="color:#4caf50;font-size:.9rem;font-weight:700">'
                    f'✅ {_n_sel} منتج محدد — اختر إجراء:</span></div>',
                    unsafe_allow_html=True,
                )
                _sel_df = filtered.iloc[sorted(st.session_state.miss_sel)].copy()
                _ac1, _ac2, _ac3, _ac4 = st.columns(4)

                with _ac1:
                    _sel_csv = export_to_salla_shamel(_sel_df, generate_descriptions=False)
                    st.download_button(
                        f"📥 سلة الشامل ({_n_sel})",
                        data=_sel_csv,
                        file_name="selected_salla.csv",
                        mime="text/csv",
                        key="miss_sel_salla",
                        use_container_width=True,
                        type="primary",
                    )
                with _ac2:
                    st.download_button(
                        f"📥 Excel ({_n_sel})",
                        data=export_to_excel(_sel_df, "مفقودة"),
                        file_name="selected_missing.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="miss_sel_xl",
                        use_container_width=True,
                    )
                with _ac3:
                    if st.button(f"🪄 وصف AI ({_n_sel})", key="miss_sel_ai",
                                 type="primary", use_container_width=True):
                        _srows = _sel_df.copy()
                        _srows["الوصف_الآلي"] = ""
                        _prog = st.progress(0, text="جاري توليد الوصف...")
                        _tot = len(_srows)
                        for _si, (_ri, _rr) in enumerate(_srows.iterrows()):
                            _pn  = str(_rr.get("منتج_المنافس", "") or "").strip()
                            _rd  = str(_rr.get("raw_description", "") or "").strip()
                            _prog.progress(_si / _tot, text=f"وصف {_si+1}/{_tot}: {_pn[:30]}…")
                            _srows.at[_ri, "الوصف_الآلي"] = generate_salla_html_description(_pn, _rd)
                        _prog.progress(1.0, text="✅ اكتمل!")
                        with st.spinner("🔗 مطابقة سلة..."):
                            _srows = map_salla_categories(_srows)
                            _srows, _mb = validate_salla_brands(_srows)
                        st.session_state["ai_gen_result_df"]      = _srows
                        st.session_state["ai_gen_missing_brands"] = _mb
                        st.session_state.pop("brands_salla_df", None)
                        st.success(f"✅ تم توليد الوصف لـ {_tot} منتج!")

                with _ac4:
                    if st.button(f"📤 Make ({_n_sel})", key="miss_sel_make",
                                 use_container_width=True):
                        is_valid, issues = validate_export_product_dataframe(_sel_df)
                        if not is_valid:
                            st.error("❌ البيانات لا تطابق معايير سلة")
                        else:
                            _sel_prods = export_to_make_format(_sel_df, "missing")
                            _pb = st.progress(0, text="جاري الإرسال...")
                            _st_ph = st.empty()

                            def _prog_cb(s, f, t, cn):
                                _pb.progress(min((s + f) / max(t, 1), 1.0))
                                _st_ph.caption(f"✅ {s} | ❌ {f} / {t}")

                            _res = send_batch_smart(_sel_prods, "new", 20, 3, _prog_cb)
                            _pb.progress(1.0)
                            if _res["success"]:
                                st.success(_res["message"])
                                st.session_state.miss_sel.clear()
                            else:
                                st.error(_res["message"])

                # ── نتيجة توليد الوصف ────────────────────────────────────
                if st.session_state.get("ai_gen_result_df") is not None:
                    _gen_df = st.session_state["ai_gen_result_df"]
                    _miss_brands = st.session_state.get("ai_gen_missing_brands") or []
                    if _miss_brands:
                        st.warning(f"⚠️ **{len(_miss_brands)} ماركة** غير مسجلة في سلة — أضفها قبل الرفع.")
                        _mbc1, _mbc2 = st.columns(2)
                        with _mbc1:
                            st.download_button(
                                "📥 أسماء الماركات (سريع)",
                                data=pd.DataFrame({"اسم الماركة": _miss_brands}).to_csv(index=False).encode("utf-8-sig"),
                                file_name="new_brands_names.csv",
                                mime="text/csv",
                                key="ai_nb_names",
                                use_container_width=True,
                            )
                        with _mbc2:
                            if st.button("🪄 قالب سلة للماركات", key="ai_brand_tpl",
                                         use_container_width=True):
                                _brows = []
                                with st.spinner("جاري التوليد..."):
                                    for _br in _miss_brands:
                                        b_data = generate_salla_brand_info(_br)
                                        _brows.append({
                                            "اسم الماركة": b_data.get("brand_name", _br),
                                            "وصف مختصر عن الماركة": b_data.get("description", ""),
                                            "صورة شعار الماركة": "",
                                            "(إختياري) صورة البانر": "",
                                            "(Page Title) عنوان صفحة العلامة التجارية": b_data.get("seo_title", ""),
                                            "(SEO Page URL) رابط صفحة العلامة التجارية": b_data.get("seo_url", ""),
                                            "(Page Description) وصف صفحة العلامة التجارية": b_data.get("seo_desc", ""),
                                        })
                                st.session_state["brands_salla_df"] = pd.DataFrame(_brows)
                                st.success(f"✅ جُهّزت {len(_brows)} ماركة")
                        if st.session_state.get("brands_salla_df") is not None:
                            st.download_button(
                                "📥 قالب الماركات (سلة الكامل)",
                                data=st.session_state["brands_salla_df"].to_csv(index=False).encode("utf-8-sig"),
                                file_name="new_brands_to_add.csv",
                                mime="text/csv",
                                type="primary",
                                key="ai_nb_full",
                                use_container_width=True,
                            )
                    _sg = format_missing_for_salla(_gen_df)
                    if not _sg.empty:
                        st.download_button(
                            "📥 ملف سلة الكامل (مع وصف AI)",
                            data=_sg.to_csv(index=False).encode("utf-8-sig"),
                            file_name="salla_ai_descriptions.csv",
                            mime="text/csv",
                            type="primary",
                            key="ai_desc_dl",
                            use_container_width=True,
                        )

            # ── تصدير الكل (يظهر دائماً) ──────────────────────────────────
            st.markdown('<hr style="border-color:#0d1a2e;margin:18px 0">', unsafe_allow_html=True)
            with st.expander("📥 تصدير الكل", expanded=False):
                _exa, _exb, _exc = st.columns(3)
                with _exa:
                    st.download_button(
                        "📥 Excel (الكل)",
                        data=export_to_excel(filtered, "مفقودة"),
                        file_name="missing_all.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="miss_dl_all",
                        use_container_width=True,
                    )
                with _exb:
                    st.download_button(
                        "📥 سلة الشامل (الكل)",
                        data=export_to_salla_shamel(filtered, generate_descriptions=False),
                        file_name="mahwous_salla_shamel.csv",
                        mime="text/csv",
                        key="miss_salla_all",
                        use_container_width=True,
                    )
                with _exc:
                    _conf_opts = {"🟢 مؤكدة فقط": "green", "🟡 محتملة": "yellow", "🔵 الكل": ""}
                    _conf_sel  = st.selectbox("مستوى الثقة", list(_conf_opts.keys()), key="miss_conf_sel")
                    _conf_val  = _conf_opts[_conf_sel]
                    if st.button("📤 إرسال الكل لـ Make", key="miss_make_all", use_container_width=True):
                        _to_send = (
                            filtered[filtered["نوع_متاح"].str.strip() == ""]
                            if "نوع_متاح" in filtered.columns else filtered
                        )
                        is_valid, issues = validate_export_product_dataframe(_to_send)
                        if not is_valid:
                            st.error("❌ تم إيقاف الإرسال — راجع جودة البيانات")
                        else:
                            _all_ps = export_to_make_format(_to_send, "missing")
                            for _ip, _pr_r in enumerate(_all_ps):
                                if _ip < len(_to_send):
                                    _pr_r["مستوى_الثقة"] = str(_to_send.iloc[_ip].get("مستوى_الثقة", "green"))
                            _pb3 = st.progress(0, text="جاري الإرسال...")
                            _st3 = st.empty()

                            def _prog_all(s, f, t, cn):
                                _pb3.progress(min((s + f) / max(t, 1), 1.0))
                                _st3.caption(f"✅ {s} | ❌ {f} / {t}")

                            _res3 = send_batch_smart(_all_ps, "new", 20, 3, _prog_all, _conf_val)
                            _pb3.progress(1.0)
                            if _res3["success"]:
                                st.success(_res3["message"])
                            else:
                                st.error(_res3["message"])
'''

# ── Splice ──────────────────────────────────────────────────────────────────
before = lines[:start]
after  = lines[end:]
new_lines = before + [NEW] + after

with open(OUT, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Done. File written. Old section: {end - start} lines → new: {len(NEW.splitlines())} lines")
