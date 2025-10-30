# Scaling Guide v2: Diff Summary

**Date:** 2025-10-30
**Comparison:** v1 (`scaling-guide-update.md`) vs v2 (`scaling-guide-update-v2.md`)

## Executive Summary

Version 2 adopts a **per-bridge resource model** (100 MB base + variable overhead) instead of the discrete tier approach in v1. This provides operators with a more flexible and predictable framework for resource planning across different deployment patterns.

---

## Major Structural Changes

### Framework Approach

**v1: Discrete Tiers**
- Quick Start table with 4 fixed tiers (1k/10k/50k/100k)
- Resources specified per tier
- Users choose closest matching tier

**v2: Per-Bridge Resource Model**
- Fixed overhead: 100 MB + 0.1 cores (every bridge)
- Variable overhead: Scales with throughput and complexity
- Reference examples with measured performance
- Rule-of-thumb formulas for custom sizing

**Why v2 is better:**
- More accurate for non-standard configurations (e.g., 7k nodes, 35k nodes)
- Clearer cost model (base + variable = total)
- Easier to explain resource allocation to management
- Better for capacity planning across multiple bridges

### Content Organization

**Sections changed:**

1. **"Quick Start: Resource Sizing Table" (v1) → "Resource Requirements per Bridge" (v2)**
   - v1: 4-row table with discrete tiers
   - v2: Base overhead explanation + 4 reference examples with full context
   - Added: Per-message overhead calculations for each example

2. **"Scaling Characteristics" (v1) → Integrated into reference examples (v2)**
   - v1: Separate section with performance data
   - v2: Performance data included within each reference example
   - Kept: Scaling trends (superlinear, sublinear) but contextualized

3. **"tag_processor Performance" (v1) → "tag_processor Complexity Impact" (v2)**
   - v1: Focused on proving tag_processor is efficient
   - v2: Focused on complexity limits and adjustment factors
   - Added: Memory multipliers and CPU additions table

4. **"Scaling Decision Tree" (v1) → "Scaling Decision Guide" (v2)**
   - v1: Flowchart format (Q&A style)
   - v2: Step-by-step guide with formulas
   - v2 includes: Step 4 (Add Production Headroom) - new section

5. **"Production Deployment Checklist" (v1 & v2)**
   - Mostly unchanged, reorganized slightly
   - v2 adds: Resource allocation calculation worksheets

6. **"Known Issues" (v1) → "Known Limitations" (v2)**
   - Same content, slightly reordered
   - v2 adds more detail on workarounds

7. **New in v2: "Summary for Operators"**
   - Quick resource estimation formulas
   - When to use each reference example
   - Common deployment patterns

---

## Section-by-Section Comparison

### Section 1: Introduction / Executive Summary

**What changed:**
- v1: Focuses on "superlinear scaling" as headline
- v2: Focuses on "per-bridge resource model" as framework
- Both: Same test evidence cited

**What stayed:**
- Key findings (superlinear throughput, sublinear CPU, JavaScript impact)
- Test dates and branch references

**Assessment:** v2 better sets expectations for the framework being presented.

---

### Section 2: Resource Requirements

**v1: "Quick Start: Resource Sizing Table"**
```markdown
| Node Count | CPU Cores | Memory | Expected Throughput |
|------------|-----------|---------|---------------------|
| 1k | 1 core | 512 MB | 500-600 msg/sec |
| 10k | 2-4 cores | 1-2 GB | 7,000-8,000 msg/sec |
| 50k | 4-8 cores | 2-4 GB | 35,000-40,000 msg/sec |
| 100k* | 8-16 cores | 4-8 GB | 70,000-80,000 msg/sec |
```

**v2: "Resource Requirements per Bridge"**
```markdown
### Base Overhead
- Fixed memory: 100 MB (Benthos runtime)
- Fixed CPU: ~0.1 cores (idle overhead)

### Reference Examples

#### Small Bridge (1,000 tags)
- Memory: 100 MB (base) + 50 MB (processing) = 150 MB total
- CPU: 0.1 + 0.05 cores = 0.15 cores
- Throughput: ~500-600 msg/sec

[Full configuration details + measured performance]
```

**What changed:**
- v2 explicitly shows base + variable breakdown
- v2 includes full configuration context for each example
- v2 adds per-message overhead calculations
- v2 includes measured performance data within examples

**What stayed:**
- Same total resource numbers (v1: 512 MB ≈ v2: 150 MB + headroom)
- Same throughput expectations
- Same node count tiers

**Assessment:** v2 provides more context and transparency about resource composition.

---

### Section 3: Scaling Characteristics

**v1: Dedicated section (4 subsections)**
- Throughput Scaling (EXCELLENT) - table format
- CPU Scaling (EXCELLENT) - table format
- Memory Scaling (GOOD) - table format
- Garbage Collection (EXCELLENT) - table format

**v2: Integrated into reference examples**
- Measured performance data shown per example
- Scaling characteristics explained in narrative form
- Same data, different presentation

**What changed:**
- v2 integrates data into reference examples (less separation)
- v2 adds "Per-message overhead" calculations
- v2 emphasizes the resource model (base + variable)

**What stayed:**
- All performance numbers (throughput, CPU, memory, GC)
- Scaling assessments (superlinear, sublinear, acceptable, improved)
- Test evidence citations

**Assessment:** v2 is less comprehensive in this section but compensates by integrating data throughout.

---

### Section 4: tag_processor Complexity

**v1: "tag_processor Performance"**
- Baseline comparison (tag_processor vs logging)
- JavaScript execution overhead (1.30% CPU)
- tag_processor breakdown table
- JavaScript complexity guidelines (separate section later)

**v2: "tag_processor Complexity Impact"**
- Complexity categories (Simple/Moderate/High/Very High)
- Memory multipliers and CPU additions table
- Example code for each complexity level
- Refactoring strategies

**What changed:**
- v2 focuses on complexity adjustments (multipliers)
- v2 organizes by complexity level, not by component
- v2 adds "Very High Complexity" category with 900 if condition data
- v2 removes baseline comparison with logging (less relevant to operators)

**What stayed:**
- JavaScript overhead data (1.30% for simple scripts)
- 900 if condition test results (19% drop, 177x memory)
- Refactoring recommendations
- Code examples (good vs bad JavaScript)

**Assessment:** v2 is more actionable for operators deciding on complexity limits.

---

### Section 5: Decision Framework

**v1: "Scaling Decision Tree"**
```
Q: What throughput do you need?
├─ < 1,000 msg/sec → 1-2 cores, 512 MB
├─ 1,000-10,000 msg/sec → 2-4 cores, 1-2 GB
└─ ...

Q: Is your JavaScript complex?
Q: Do you need > 50k nodes?
```

**v2: "Scaling Decision Guide"**
```
Step 1: Determine Message Throughput
  [Formula: fast + slow nodes]

Step 2: Estimate Base Resources
  [Reference example table]

Step 3: Adjust for JavaScript Complexity
  [Multiplier table]

Step 4: Add Production Headroom
  [Memory 1.5x, CPU 2x]

Step 5: Validate Against Tested Configurations
  [Tested limits checklist]
```

**What changed:**
- v2 uses step-by-step process instead of Q&A tree
- v2 adds "Add Production Headroom" as explicit step
- v2 includes throughput calculation formula
- v2 emphasizes validation against tested configs

**What stayed:**
- Core decision factors (throughput, complexity, node count)
- Resource allocation recommendations
- Same numerical outcomes

**Assessment:** v2 is more procedural (better for operators), v1 is more exploratory (better for understanding).

---

### Section 6: Production Checklist

**v1 & v2: Mostly identical**

Both include:
- Resource Allocation checklist
- Configuration Validation checklist
- Monitoring Setup checklist
- Post-Deployment Validation checklist

**Minor differences:**
- v2 adds worksheet blanks ("CPU: `____` cores")
- v2 emphasizes "minimum from calculation" (ties back to decision guide)
- v1 has slightly more detail in monitoring section

**Assessment:** Functionally equivalent, v2 slightly more actionable.

---

### Section 7: Known Issues/Limitations

**v1: "Known Issues and Limitations"**
- 100k Node Deadlock (ENG-3799) - detailed
- Throughput Variability - brief
- tag_processor Limitations - detailed

**v2: "Known Limitations"**
- Same three subsections
- Slightly more detail on workarounds
- Emphasizes "Use horizontal scaling until fix" more prominently

**Assessment:** v2 is slightly clearer on remediation paths.

---

### Section 8: Advanced Topics

**v1: "Performance Optimization Guide"**
- When to Optimize (decision criteria)
- Quick Wins (0-2 hours)
- Advanced Optimizations (8-40 hours)
- What NOT to Optimize

**v2: "Advanced: Scaling Beyond 50k"**
- 100k Feasibility Analysis (detailed projections)
- Horizontal Scaling (multiple bridges pattern)
- Trade-offs comparison (advantages/disadvantages)

**What changed:**
- v2 removes optimization guide (less relevant to resource planning)
- v2 focuses on scaling strategies (horizontal vs vertical)
- v2 adds explicit deployment pattern example

**What stayed:**
- 100k feasibility data (same projections)
- Horizontal scaling recommendations

**Assessment:** v2 is more focused on scaling strategies, v1 is more focused on performance tuning.

---

### Section 9: Summary

**v1: "Summary for Documentation Team"**
- What Changed (from previous docs)
- Why These Improvements Matter (UX principles)
- Proposed Documentation Structure

**v2: "Summary for Operators"**
- Quick Resource Estimation (rule-of-thumb formulas)
- When to Use Each Reference Example
- Common Deployment Patterns (per-machine, per-line, per-plant)

**What changed:**
- v2 targets operators, v1 targets documentation team
- v2 provides quick formulas, v1 provides meta-analysis
- v2 adds deployment pattern guidance

**Assessment:** Different audiences, both valuable.

---

## Key Data Points: Comparison Table

| Metric | v1 Location | v2 Location | Changed? |
|--------|-------------|-------------|----------|
| **1k node throughput** | Quick Start table | Small bridge example | No (596 msg/sec) |
| **10k node throughput** | Quick Start table | Medium bridge example | No (7,280 msg/sec) |
| **50k node throughput** | Quick Start table | Large bridge example | No (38,837 msg/sec) |
| **Base memory overhead** | Not explicit | 100 MB (stated) | NEW |
| **Variable memory overhead** | Implicit | Calculated per example | NEW |
| **GC improvement** | Scaling section | Mentioned in 50k example | Same data |
| **JavaScript overhead** | tag_processor section | Complexity table | Same data |
| **900 if condition impact** | Complexity section | Very High Complexity | Same data |
| **Horizontal scaling** | Brief mention | Dedicated subsection | Expanded |

---

## Terminology Changes

| v1 Term | v2 Term | Reason |
|---------|---------|--------|
| "Quick Start: Resource Sizing Table" | "Resource Requirements per Bridge" | Emphasizes per-bridge model |
| "Scaling Characteristics" | (Integrated) | Data moved into reference examples |
| "tag_processor Performance" | "tag_processor Complexity Impact" | Focus on complexity adjustments |
| "Scaling Decision Tree" | "Scaling Decision Guide" | Step-by-step process vs Q&A |
| "Known Issues" | "Known Limitations" | More neutral tone |

---

## UX Standards Compliance

### Immediate Trust (Real Values)

**v1:**
- ✅ Shows real throughput: "7,280 msg/sec"
- ✅ Shows real CPU: "33.18% utilization"
- ✅ Admits limitations: "100k blocked by ENG-3799"

**v2:**
- ✅ Same real values
- ✅ Adds per-message overhead: "22.8 KB/msg"
- ✅ Shows base + variable breakdown: "100 MB + 400 MB"

**Assessment:** v2 slightly better (more transparency about resource composition).

### Opinionated Simplicity (One Way)

**v1:**
- Decision tree with 3 questions
- Quick Start table for immediate answers
- Some flexibility in interpretation

**v2:**
- 5-step decision guide (more prescriptive)
- Reference examples as starting points
- Formulas for custom scenarios

**Assessment:** v2 more opinionated (step-by-step process), v1 more flexible.

### Progressive Power (Clicks → Code)

**v1:**
- Level 1: Quick Start table (30s)
- Level 2: Decision tree (5 min)
- Level 3: Detailed sections (30 min)
- Level 4: Appendix (experts)

**v2:**
- Level 1: Reference examples (30s)
- Level 2: Decision guide (5 min)
- Level 3: Operator summary (10 min)
- Level 4: Appendix (experts)

**Assessment:** Both provide progressive disclosure, different entry points.

### Error Excellence (Actionable)

**v1:**
- Warnings about JavaScript complexity
- 100k deadlock with workaround
- Production checklist

**v2:**
- Same warnings, more prominent
- Workaround emphasized earlier
- Deployment patterns guide

**Assessment:** v2 slightly more actionable (patterns and formulas).

---

## Recommendations

### When to Use v1

**Best for:**
- Users familiar with discrete tier models (small/medium/large)
- Documentation that explains "why" scaling works
- Technical deep-dives into performance characteristics
- Showcasing test evidence to stakeholders

**Structure:**
- Comprehensive scaling analysis
- Detailed performance breakdowns
- UX-focused summary for documentation team

### When to Use v2

**Best for:**
- Operators planning resource allocation
- Custom deployments (non-standard node counts)
- Capacity planning across multiple bridges
- Quick estimation without deep analysis

**Structure:**
- Per-bridge resource model (base + variable)
- Reference examples with full context
- Rule-of-thumb formulas for operators
- Deployment pattern guidance

### Hybrid Approach

**Option: Combine both documents**

```markdown
## Quick Start
[v2 reference examples - immediate clarity]

## Understanding Scaling
[v1 scaling characteristics - deep dive]

## Resource Planning
[v2 decision guide + formulas - operational]

## Deployment Patterns
[v2 operator summary - practical guidance]

## Production Checklist
[v1 + v2 merged - comprehensive]

## Appendix
[v1 test methodology - evidence base]
```

**Benefits:**
- Reference examples provide immediate answers (v2)
- Scaling characteristics explain the "why" (v1)
- Decision guide provides step-by-step process (v2)
- Test methodology provides credibility (v1)

---

## Final Assessment

### v1 Strengths
- ✅ Comprehensive performance analysis
- ✅ Explains scaling trends in detail
- ✅ Strong UX principles application
- ✅ Detailed test evidence presentation

### v1 Weaknesses
- ⚠️ Discrete tiers less flexible for custom scenarios
- ⚠️ Longer read time to get to actionable numbers
- ⚠️ Optimization section may be premature for resource planning

### v2 Strengths
- ✅ Per-bridge model more intuitive for operators
- ✅ Reference examples provide full context
- ✅ Quick formulas for estimation
- ✅ Deployment pattern guidance

### v2 Weaknesses
- ⚠️ Less comprehensive on scaling theory
- ⚠️ Performance data integrated (harder to compare across scales)
- ⚠️ Removes optimization guide (may need separate doc)

### Recommendation

**For docs.umh.app publication:**
- Use **v2 as primary document** (better for operators)
- Link to **v1 as "Deep Dive: Scaling Analysis"** (for those who want details)
- Consider **hybrid approach** (best of both)

**Rationale:**
- Operators need quick, actionable resource planning (v2)
- Engineers need performance justification (v1)
- Both audiences are important, serve them with appropriate depth

---

## Change Log

**What sections changed significantly:**
1. ✅ Resource Requirements - Complete rewrite (per-bridge model)
2. ✅ Scaling Characteristics - Restructured (integrated into examples)
3. ✅ tag_processor section - Refocused (complexity impact)
4. ✅ Decision framework - Reorganized (step-by-step guide)
5. ⚠️ Production checklist - Minor tweaks
6. ⚠️ Known issues - Minor expansion
7. ✅ Advanced topics - Replaced (optimization → horizontal scaling)
8. ✅ Summary - Complete rewrite (operators vs documentation team)

**What stayed the same:**
- All performance numbers and test data
- JavaScript complexity warnings
- 100k deadlock issue and workaround
- Test methodology appendix
- Core recommendations (< 50k safe, horizontal scaling for > 50k)

**Total rewrite percentage:** ~60% (structure and presentation), ~10% (actual data)

---

**Document Status:** ✅ Ready for review
**Next Step:** Choose publication strategy (v2 primary, v1 deep-dive, or hybrid)
