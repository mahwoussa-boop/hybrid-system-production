import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

start = None
end = None
for i, l in enumerate(lines):
    if 'elif page ==' in l and '\U0001f50d' in l and 'مفقودة' in l:
        start = i
    if start and i > start and 'elif page ==' in l and 'مفقودة' not in l:
        end = i
        break

print(f'start={start+1} end={end}')
print(f'section lines: {end - start}')
