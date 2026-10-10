//! A small labelled relevance set, generated deterministically.
//!
//! 24 topics, each with its own entities, nouns, verbs (base, past and
//! gerund forms) and three aspects of two words. Every topic has 16 claims
//! (each about one aspect), and 64 distractor claims are made of words
//! shared by every topic ("officials said on Tuesday ..."). Claims mix case,
//! punctuation, possessives and inflected verb forms; about a third also
//! mention a noun of another topic, so sharing one word with a query is
//! weak evidence of relevance. 448 claims in all.
//!
//! Queries (72, three per topic) use other surface forms than the claims
//! (base verb form where claims use the past or gerund, plurals, upper
//! case, punctuation, stop words):
//! - aspect: `"{verb} {noun} {aspect words}"`, grade 2 for claims of the
//!   topic and aspect, 1 for the topic's other claims;
//! - entity: `"what did {entity} say about the {noun}?"`, grade 2 for
//!   claims of the topic naming that entity, 1 for the topic's other claims;
//! - keywords: `"{NOUN}S, {noun}!"`, grade 1 for every claim of the topic.
//!
//! Everything else is grade 0. The generator uses its own SplitMix64, so the
//! set does not change with the `rand` crate.

#![allow(dead_code)]

use std::collections::HashMap;

pub struct Topic {
    pub entities: [&'static str; 2],
    pub nouns: [&'static str; 4],
    /// (base, past, gerund)
    pub verbs: [(&'static str, &'static str, &'static str); 2],
    pub aspects: [[&'static str; 2]; 3],
}

pub const TOPICS: &[Topic] = &[
    Topic {
        entities: ["Helios", "Nova"],
        nouns: ["merger", "shareholder", "acquisition", "board"],
        verbs: [
            ("acquire", "acquired", "acquiring"),
            ("approve", "approved", "approving"),
        ],
        aspects: [
            ["antitrust", "regulator"],
            ["valuation", "premium"],
            ["layoffs", "integration"],
        ],
    },
    Topic {
        entities: ["Corvane", "Ridgeport"],
        nouns: ["reactor", "coolant", "pump", "turbine"],
        verbs: [
            ("inspect", "inspected", "inspecting"),
            ("shut", "shut", "shutting"),
        ],
        aspects: [
            ["leak", "valve"],
            ["outage", "grid"],
            ["radiation", "monitor"],
        ],
    },
    Topic {
        entities: ["Lisbon", "Porto"],
        nouns: ["rainfall", "storm", "flood", "forecast"],
        verbs: [
            ("warn", "warned", "warning"),
            ("evacuate", "evacuated", "evacuating"),
        ],
        aspects: [
            ["river", "levee"],
            ["wind", "gust"],
            ["drought", "reservoir"],
        ],
    },
    Topic {
        entities: ["Aldermoor", "Quayside"],
        nouns: ["bridge", "toll", "highway", "traffic"],
        verbs: [
            ("build", "built", "building"),
            ("raise", "raised", "raising"),
        ],
        aspects: [
            ["congestion", "commuter"],
            ["steel", "cable"],
            ["budget", "contractor"],
        ],
    },
    Topic {
        entities: ["Vireo", "Calder"],
        nouns: ["vaccine", "trial", "dose", "patient"],
        verbs: [
            ("test", "tested", "testing"),
            ("enroll", "enrolled", "enrolling"),
        ],
        aspects: [
            ["efficacy", "placebo"],
            ["adverse", "fever"],
            ["booster", "elderly"],
        ],
    },
    Topic {
        entities: ["Brightwater", "Okafor"],
        nouns: ["bond", "yield", "inflation", "rate"],
        verbs: [("cut", "cut", "cutting"), ("hike", "hiked", "hiking")],
        aspects: [
            ["treasury", "auction"],
            ["mortgage", "housing"],
            ["currency", "exchange"],
        ],
    },
    Topic {
        entities: ["Stellan", "Mirabel"],
        nouns: ["satellite", "rocket", "orbit", "launch"],
        verbs: [
            ("launch", "launched", "launching"),
            ("deploy", "deployed", "deploying"),
        ],
        aspects: [
            ["booster", "landing"],
            ["antenna", "signal"],
            ["debris", "collision"],
        ],
    },
    Topic {
        entities: ["Granfield", "Tessaro"],
        nouns: ["harvest", "wheat", "crop", "farmer"],
        verbs: [
            ("plant", "planted", "planting"),
            ("export", "exported", "exporting"),
        ],
        aspects: [
            ["fertilizer", "soil"],
            ["locust", "pest"],
            ["subsidy", "tariff"],
        ],
    },
    Topic {
        entities: ["Kestrel", "Umbra"],
        nouns: ["malware", "breach", "firewall", "server"],
        verbs: [
            ("patch", "patched", "patching"),
            ("encrypt", "encrypted", "encrypting"),
        ],
        aspects: [
            ["ransom", "bitcoin"],
            ["phishing", "password"],
            ["botnet", "traffic"],
        ],
    },
    Topic {
        entities: ["Marisol", "Dunmore"],
        nouns: ["election", "ballot", "voter", "candidate"],
        verbs: [
            ("vote", "voted", "voting"),
            ("campaign", "campaigned", "campaigning"),
        ],
        aspects: [
            ["turnout", "precinct"],
            ["debate", "televised"],
            ["recount", "margin"],
        ],
    },
    Topic {
        entities: ["Ashgrove", "Pellucid"],
        nouns: ["museum", "painting", "exhibition", "gallery"],
        verbs: [
            ("restore", "restored", "restoring"),
            ("exhibit", "exhibited", "exhibiting"),
        ],
        aspects: [
            ["forgery", "authenticity"],
            ["auction", "collector"],
            ["sculpture", "bronze"],
        ],
    },
    Topic {
        entities: ["Thornbury", "Velasco"],
        nouns: ["railway", "train", "station", "passenger"],
        verbs: [
            ("derail", "derailed", "derailing"),
            ("electrify", "electrified", "electrifying"),
        ],
        aspects: [
            ["signal", "fault"],
            ["fare", "ticket"],
            ["tunnel", "excavation"],
        ],
    },
    Topic {
        entities: ["Okoro", "Lindqvist"],
        nouns: ["glacier", "ice", "climate", "temperature"],
        verbs: [
            ("melt", "melted", "melting"),
            ("measure", "measured", "measuring"),
        ],
        aspects: [
            ["sea", "level"],
            ["permafrost", "methane"],
            ["snowfall", "alpine"],
        ],
    },
    Topic {
        entities: ["Harrowgate", "Sunbeam"],
        nouns: ["factory", "battery", "lithium", "cell"],
        verbs: [
            ("manufacture", "manufactured", "manufacturing"),
            ("recycle", "recycled", "recycling"),
        ],
        aspects: [
            ["cobalt", "mine"],
            ["charging", "capacity"],
            ["fire", "thermal"],
        ],
    },
    Topic {
        entities: ["Penhallow", "Rook"],
        nouns: ["hospital", "nurse", "surgery", "ward"],
        verbs: [
            ("treat", "treated", "treating"),
            ("admit", "admitted", "admitting"),
        ],
        aspects: [
            ["waiting", "backlog"],
            ["strike", "wages"],
            ["infection", "sterile"],
        ],
    },
    Topic {
        entities: ["Calloway", "Ibsen"],
        nouns: ["football", "striker", "league", "goal"],
        verbs: [
            ("score", "scored", "scoring"),
            ("transfer", "transferred", "transferring"),
        ],
        aspects: [
            ["injury", "hamstring"],
            ["referee", "penalty"],
            ["stadium", "fans"],
        ],
    },
    Topic {
        entities: ["Westmarch", "Delacroix"],
        nouns: ["tax", "deficit", "spending", "parliament"],
        verbs: [
            ("legislate", "legislated", "legislating"),
            ("reform", "reformed", "reforming"),
        ],
        aspects: [
            ["pension", "retirement"],
            ["corporate", "loophole"],
            ["austerity", "welfare"],
        ],
    },
    Topic {
        entities: ["Quillon", "Marrak"],
        nouns: ["earthquake", "tremor", "fault", "seismic"],
        verbs: [
            ("strike", "struck", "striking"),
            ("collapse", "collapsed", "collapsing"),
        ],
        aspects: [
            ["tsunami", "coast"],
            ["aftershock", "magnitude"],
            ["rubble", "rescue"],
        ],
    },
    Topic {
        entities: ["Bramble", "Sorensen"],
        nouns: ["smartphone", "chip", "processor", "display"],
        verbs: [
            ("release", "released", "releasing"),
            ("unveil", "unveiled", "unveiling"),
        ],
        aspects: [
            ["camera", "sensor"],
            ["benchmark", "performance"],
            ["recall", "overheating"],
        ],
    },
    Topic {
        entities: ["Elmstead", "Naranjo"],
        nouns: ["coffee", "cafe", "roaster", "bean"],
        verbs: [
            ("roast", "roasted", "roasting"),
            ("brew", "brewed", "brewing"),
        ],
        aspects: [
            ["espresso", "barista"],
            ["arabica", "plantation"],
            ["price", "shortage"],
        ],
    },
    Topic {
        entities: ["Fairhaven", "Kuroda"],
        nouns: ["shipping", "port", "container", "vessel"],
        verbs: [
            ("dock", "docked", "docking"),
            ("reroute", "rerouted", "rerouting"),
        ],
        aspects: [
            ["canal", "blockage"],
            ["freight", "costs"],
            ["piracy", "escort"],
        ],
    },
    Topic {
        entities: ["Glenrock", "Abernathy"],
        nouns: ["school", "teacher", "student", "curriculum"],
        verbs: [
            ("teach", "taught", "teaching"),
            ("graduate", "graduated", "graduating"),
        ],
        aspects: [
            ["exam", "grades"],
            ["funding", "classroom"],
            ["literacy", "reading"],
        ],
    },
    Topic {
        entities: ["Starling", "Moravec"],
        nouns: ["robot", "warehouse", "automation", "drone"],
        verbs: [
            ("automate", "automated", "automating"),
            ("deliver", "delivered", "delivering"),
        ],
        aspects: [
            ["parcel", "delivery"],
            ["safety", "collision"],
            ["jobs", "workers"],
        ],
    },
    Topic {
        entities: ["Copperfield", "Yusuf"],
        nouns: ["wildfire", "forest", "firefighter", "smoke"],
        verbs: [
            ("burn", "burned", "burning"),
            ("contain", "contained", "containing"),
        ],
        aspects: [
            ["evacuation", "homes"],
            ["air", "quality"],
            ["arson", "investigation"],
        ],
    },
];

/// Words every topic uses.
const COMMON: &[&str] = &[
    "officials",
    "said",
    "report",
    "new",
    "plan",
    "year",
    "according",
    "statement",
    "week",
    "data",
    "group",
    "Tuesday",
    "local",
    "national",
    "announced",
    "expected",
    "people",
    "government",
    "company",
    "today",
    "major",
    "first",
    "after",
    "recent",
    "update",
];

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

pub struct EvalClaim {
    pub id: String,
    pub text: String,
    pub confidence: f32,
    /// `None` for distractors.
    pub topic: Option<usize>,
    pub aspect: usize,
    pub entity: usize,
}

pub struct EvalQuery {
    pub text: String,
    /// Graded relevance (1 or 2) of every relevant claim id.
    pub relevant: HashMap<String, u8>,
}

pub struct RelevanceSet {
    pub claims: Vec<EvalClaim>,
    pub queries: Vec<EvalQuery>,
}

fn capitalise(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

pub fn relevance_set() -> RelevanceSet {
    let mut rng = SplitMix64(0x00D4_5EED);
    let mut claims = Vec::new();
    for (t, topic) in TOPICS.iter().enumerate() {
        for n in 0..16 {
            let aspect = n % 3;
            let entity = rng.below(2);
            let ent = topic.entities[entity];
            let noun = *rng.pick(&topic.nouns);
            let noun2 = *rng.pick(&topic.nouns);
            let (_, past, gerund) = *rng.pick(&topic.verbs);
            let [a1, a2] = topic.aspects[aspect];
            let c1 = *rng.pick(COMMON);
            let c2 = *rng.pick(COMMON);
            let c3 = *rng.pick(COMMON);
            let mut text = match rng.below(4) {
                0 => format!("{ent} {past} the {noun} after {a1} concerns, {c1} {c2} said."),
                1 => format!(
                    "{} {c1}: {ent}'s {noun} {a2} {gerund} {noun2} {c2} {c3}",
                    capitalise(c3)
                ),
                2 => format!("The {noun} {a1} and {a2} {c1} by {ent} ({c2} {c3})."),
                _ => format!(
                    "{} {ent} is {gerund} {noun2}s; {a2} {c1} {c2}.",
                    capitalise(c1)
                ),
            };
            if rng.chance(33) {
                let other = &TOPICS[(t + 1 + rng.below(TOPICS.len() - 1)) % TOPICS.len()];
                text.push_str(&format!(" Also mentions {}.", rng.pick(&other.nouns)));
            }
            if rng.chance(20) {
                text = text.to_uppercase();
            }
            claims.push(EvalClaim {
                id: format!("t{t:02}-c{n:02}"),
                text,
                confidence: 0.5 + (rng.below(51) as f32) / 100.0,
                topic: Some(t),
                aspect,
                entity,
            });
        }
    }
    for n in 0..64 {
        let words: Vec<&str> = (0..8).map(|_| *rng.pick(COMMON)).collect();
        let mut text = format!("{}.", words.join(" "));
        if rng.chance(50) {
            let other = rng.pick(TOPICS);
            text.push_str(&format!(" The {} was discussed.", rng.pick(&other.nouns)));
        }
        claims.push(EvalClaim {
            id: format!("d-c{n:02}"),
            text: capitalise(&text),
            confidence: 0.5 + (rng.below(51) as f32) / 100.0,
            topic: None,
            aspect: 0,
            entity: 0,
        });
    }

    let mut queries = Vec::new();
    for (t, topic) in TOPICS.iter().enumerate() {
        let in_topic = |claim: &EvalClaim| claim.topic == Some(t);
        // Aspect query, base verb form.
        let aspect = rng.below(3);
        let (base, _, _) = *rng.pick(&topic.verbs);
        let noun = *rng.pick(&topic.nouns);
        let [a1, a2] = topic.aspects[aspect];
        let mut relevant = HashMap::new();
        for claim in claims.iter().filter(|c| in_topic(c)) {
            relevant.insert(claim.id.clone(), if claim.aspect == aspect { 2 } else { 1 });
        }
        queries.push(EvalQuery {
            text: format!("{base} {noun} {a1} {a2}"),
            relevant,
        });
        // Entity query with stop words.
        let entity = rng.below(2);
        let noun = *rng.pick(&topic.nouns);
        let mut relevant = HashMap::new();
        for claim in claims.iter().filter(|c| in_topic(c)) {
            relevant.insert(claim.id.clone(), if claim.entity == entity { 2 } else { 1 });
        }
        queries.push(EvalQuery {
            text: format!("what did {} say about the {noun}?", topic.entities[entity]),
            relevant,
        });
        // Keywords: plural, upper case, punctuation.
        let n1 = *rng.pick(&topic.nouns);
        let n2 = *rng.pick(&topic.nouns);
        let relevant = claims
            .iter()
            .filter(|c| in_topic(c))
            .map(|c| (c.id.clone(), 1))
            .collect();
        queries.push(EvalQuery {
            text: format!("{}S, {n2}!", n1.to_uppercase()),
            relevant,
        });
    }
    RelevanceSet { claims, queries }
}

/// nDCG@k with gain `2^grade - 1` and a `log2(rank + 2)` discount.
pub fn ndcg_at(k: usize, ranked: &[String], relevant: &HashMap<String, u8>) -> f64 {
    let gain = |grade: u8| f64::from((1u32 << grade) - 1);
    let dcg: f64 = ranked
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, id)| gain(relevant.get(id).copied().unwrap_or(0)) / ((i + 2) as f64).log2())
        .sum();
    let mut grades: Vec<u8> = relevant.values().copied().collect();
    grades.sort_unstable_by(|a, b| b.cmp(a));
    let ideal: f64 = grades
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, g)| gain(*g) / ((i + 2) as f64).log2())
        .sum();
    if ideal == 0.0 { 0.0 } else { dcg / ideal }
}

/// Recall@k capped at k: relevant claims in the top k divided by
/// `min(k, number of relevant claims)`.
pub fn recall_at(k: usize, ranked: &[String], relevant: &HashMap<String, u8>) -> f64 {
    let hits = ranked
        .iter()
        .take(k)
        .filter(|id| relevant.contains_key(*id))
        .count();
    let denom = relevant.len().min(k);
    if denom == 0 {
        0.0
    } else {
        hits as f64 / denom as f64
    }
}
