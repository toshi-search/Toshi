use std::collections::HashMap;
use std::fs::read_dir;
use std::io;
use std::iter::Iterator;
use std::path::PathBuf;

use tantivy::collector::TopCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::*;
use tantivy::Index;

use super::{Error, Result};
use handlers::search::{Queries, Search, RangeQuery};

#[derive(Serialize, Debug, Clone)]
pub struct IndexCatalog {
    base_path: PathBuf,

    #[serde(skip_serializing)]
    collection: HashMap<String, Index>,
}

#[derive(Serialize)]
pub struct SearchResults {
    // TODO: Add Timing
    // TODO: Add Shard Information
    hits: usize,
    docs: Vec<ScoredDoc>,
}

impl SearchResults {
    pub fn new(docs: Vec<ScoredDoc>) -> Self { SearchResults { hits: docs.len(), docs } }
    pub fn len(&self) -> usize { self.docs.len() }
}

#[derive(Serialize)]
pub struct ScoredDoc {
    score: f32,
    doc:   NamedFieldDocument,
}

impl ScoredDoc {
    pub fn new(score: f32, doc: NamedFieldDocument) -> Self { ScoredDoc { score, doc } }
}

impl IndexCatalog {
    pub fn new(base_path: PathBuf) -> io::Result<Self> {
        let mut index_cat = IndexCatalog {
            base_path:  base_path.clone(),
            collection: HashMap::new(),
        };
        index_cat.refresh_catalog()?;
        info!("Indexes: {:?}", index_cat.collection.keys());
        Ok(index_cat)
    }

    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_index(name: String, index: Index) -> Result<Self> {
        let mut map = HashMap::new();
        map.insert(name, index);
        Ok(IndexCatalog {
            base_path:  PathBuf::new(),
            collection: map,
        })
    }

    pub fn load_index(path: &str) -> Result<Index> {
        let p = PathBuf::from(path);
        if p.exists() {
            Index::open_in_dir(p)
                .map_err(|_| Error::UnknownIndex(format!("No Index exists at path: {}", path)))
                .and_then(|r| Ok(r))
        } else {
            Err(Error::UnknownIndex(format!("No Index exists at path: {}", path)))
        }
    }
}

impl IndexCatalog {
    pub fn add_index(&mut self, name: String, index: Index) { self.collection.insert(name, index); }

    #[allow(dead_code)]
    pub fn get_collection(&self) -> &HashMap<String, Index> { &self.collection }

    pub fn get_index(&self, name: &str) -> Result<&Index> { self.collection.get(name).ok_or_else(|| Error::UnknownIndex(name.to_string())) }

    pub fn refresh_catalog(&mut self) -> io::Result<()> {
        self.collection.clear();

        for dir in read_dir(self.base_path.clone())? {
            let entry = dir?.path();
            let entry_str = entry.to_str().unwrap();
            let pth: String = entry_str.rsplit("/").take(1).collect();
            let idx = IndexCatalog::load_index(entry_str).unwrap();
            self.add_index(pth.clone(), idx);
        }
        Ok(())
    }

    pub fn search_index(&self, index: &str, search: &Search) -> Result<SearchResults> {
        match self.get_index(index) {
            Ok(index) => {
                index.load_searchers()?;
                let searcher = index.searcher();
                let schema = index.schema();
                let fields: Vec<Field> = schema.fields().iter().map(|e| schema.get_field(e.name()).unwrap()).collect();

                let mut collector = TopCollector::with_limit(search.limit);
                let mut query_parser = QueryParser::for_index(index, fields);
                query_parser.set_conjunction_by_default();

                match &search.query {
                    Queries::TermQuery(tq) => {
                        let terms = tq
                            .term
                            .iter()
                            .map(|x| format!("{}:{}", x.0, x.1))
                            .collect::<Vec<String>>()
                            .join(" ");

                        let query = query_parser.parse_query(&terms)?;
                        info!("{}", terms);
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    Queries::RangeQuery(RangeQuery { range }) => {
                        info!("{:#?}", range);
                        let terms = range
                            .iter()
                            .map(|(field, value)| {
                                let mut term_query = format!("{}:", field);
                                let mut upper_inclusive = false;
                                let mut lower_inclusive = false;

                                if value.contains_key("gte") {
                                    lower_inclusive = true;
                                }
                                if value.contains_key("lte") {
                                    upper_inclusive = true;
                                }
                                if lower_inclusive {
                                    term_query += "[";
                                    if let Some(v) = value.get("gte") {
                                        term_query += &v.to_string();
                                    }
                                } else {
                                    term_query += "{";
                                    if let Some(v) = value.get("gt") {
                                        term_query += &v.to_string();
                                    }
                                }
                                term_query += " TO ";
                                if upper_inclusive {
                                    if let Some(v) = value.get("lte") {
                                        term_query += &v.to_string();
                                    }
                                    term_query += "]";
                                } else {
                                    if let Some(v) = value.get("lt") {
                                        term_query += &v.to_string();
                                    }
                                    term_query += "}";
                                }
                                term_query
                            })
                            .collect::<Vec<String>>()
                            .join(" ");

                        let query = query_parser.parse_query(&terms)?;
                        info!("{}", terms);
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    Queries::AllQuery => {
                        info!("Retrieving all docs...");
                        searcher.search(&AllQuery, &mut collector)?;
                    }
                    Queries::RawQuery(rq) => {
                        let query = query_parser.parse_query(&rq.raw)?;
                        info!("{:#?}", query);
                        searcher.search(&*query, &mut collector)?;
                    }
                    _ => unimplemented!(),
                };

                let scored_docs: Vec<ScoredDoc> = collector
                    .score_docs()
                    .iter()
                    .map(|(score, doc)| {
                        let d = searcher.doc(&doc).expect("Doc not found in segment");
                        ScoredDoc::new(*score, schema.to_named_doc(&d))
                    })
                    .collect();

                Ok(SearchResults::new(scored_docs))
            }
            Err(e) => Err(e),
        }
    }

    #[allow(dead_code)]
    pub fn create_index(&mut self, _path: &str, _schema: &Schema) -> Result<()> { Ok(()) }
}

// A helper function for testing with in memory Indexes. Not meant for use
// outside of testing.
#[doc(hidden)]
#[cfg(test)]
pub fn create_test_index() -> Index {
    let mut builder = SchemaBuilder::new();
    builder.add_text_field("test_text", STORED | TEXT);
    builder.add_i64_field("test_i64", INT_STORED | INT_INDEXED);
    builder.add_u64_field("test_u64", INT_STORED | INT_INDEXED);

    let schema = builder.build();
    Index::create_in_ram(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_env_logger;
    use std::env;

    #[test]
    #[ignore]
    fn test_path_splitting() {
        env::set_var("RUST_LOG", "info");
        pretty_env_logger::init();

        let test_path = PathBuf::from("./indexes/rap");

        let mut schema = SchemaBuilder::new();

        schema.add_text_field("lyrics", TEXT | STORED);
        schema.add_text_field("title", TEXT | STORED);
        schema.add_text_field("album", TEXT | STORED);
        schema.add_i64_field("year", INT_INDEXED | INT_STORED | FAST);
        let built = schema.build();

        let index = Index::create_in_dir(&test_path, built.clone()).unwrap();

        let lyrics = built.get_field("lyrics").unwrap();
        let title = built.get_field("title").unwrap();
        let album = built.get_field("album").unwrap();
        let year = built.get_field("year").unwrap();

        let doc = doc!(
            lyrics => "[Kevin Gates:] Some individuals look at the accomplishments of other individuals and allow themselves to become jealous. Everybody know what it take. Everybody don't do what it take, I don't get tired [Kevin Gates:] Sometimes you grind from the bottom, get your chips straight Sometimes you can make a million off a mixtape Sometimes you get to the top and then your family hate Don't give a damn what them haters say I feel like I'm 'posed to be ballin' Supposed to be winnin', yeah I feel like I'm 'posed to be ballin' No one gave me shit, yeah I feel like, I feel like I'm [2 Chainz:] I did a song with Kevin way 'fore ya'll followed him on his Instagram Did a song with Young Dolph 'fore ya'll even know what Memphis had I intend to smash, no pen and pad Married to the game, no strings attached Married to the game, got a season pass You wasn't here, [?] don't even [?] From Decatur to [?] I make love with a mink on I'm top floor, you need a key just to get off of Need a key just to get off of Need a key just to get off of Hanging rappers on a chop board Outline 'em in chalk Lord Thought so Trap nigga on a pop tour Break dancing on cardboard Wrist workin' that Pyrex In the bando with that [?] 50 shots with my nigga Jonny Somebody stole the truck from Benihanas Next day he bought a new one Like you win some and you lose some [Kevin Gates:] Sometimes you grind from the bottom, get your chips straight Sometimes you can make a million off a mixtape Sometimes you get to the top and then your family hate Don't give a damn what them haters say I feel like I'm 'posed to be ballin' Supposed to be winnin', yeah I feel like I'm 'posed to be ballin' No one gave me shit, yeah I feel like, I feel like I'm [2 Chainz:] I'm supposed to be ballin', 'posed to be winnin' I spent thousands on linens and this is just the beginnin' I bought my momma a crib before I got my own place Picked my pop up from prison and gave 'em places to stay See I am handpicked by God, I defied all the odds I need a sign that say foreigns only in my garage You know I'm vicious and hungry, my competition is phony I'm on my way, kowabunga To all my cousins, I love ya I work hard for this shit, I got sleep deprivation My momma tellin' me, boy you gotta take a vacation I got one in the air, another one in rotation And when they ask me where I'm at, I say the trap my location [Kevin Gates:] Sometimes you grind from the bottom, get your chips straight Sometimes you can make a million off a mixtape Sometimes you get to the top and then your family hate Don't give a damn what them haters say I feel like I'm 'posed to be ballin' Supposed to be winnin', yeah I feel like I'm 'posed to be ballin' No one gave me shit, yeah I feel like, I feel like I'm [2 Chainz:] When I dropped Trapaveli, I stayed in 3 star 'telis With 2 time felons, one time I was bailin' Every Tom, Dick, and Helen They would act like they was reppin' Who was real? Who was fake? It was hard to keep it separate And my bankroll big, this big, need excedrin And excessive marijuana in my motherfuckin' prison We would wrap it like a present We would act it like the preset You was actin' like a peasant I hold an eagle in the desert With the Rollie and the bezel I was fuckin' with the bastards Yeah I am so slick, bitch you better hit your hazards And I bought a Versace plate just to eat my salad And I can count money til I get a fuckin' callous [Kevin Gates:] Sometimes you grind from the bottom, get your chips straight Sometimes you can make a million off a mixtape Sometimes you get to the top and then your family hate Don't give a damn what them haters say I feel like I'm 'posed to be ballin' Supposed to be winnin', yeah I feel like I'm 'posed to be ballin' No one gave me shit, yeah I feel like, I feel like I'm",
            title => "I Feel Like",
            album => "Trap-A-Velli Tre",
            year => 2015i64
        );
        let doc2 = doc!(
            lyrics => "I'm the king of the trap, El Chapo Jr I'm the king of the trap, El Chapo Jr El Chapo Jr, El Chapo Jr, El Chapo Jr, El Chapo Jr I'm the king of the trap, El Chapo Jr Anywhere on the map, I bring it to you El Chapo Jr, El Chapo Jr, El Chapo Jr, I'ma bring it to ya I wear Versace like it's Nike, you don't like it do you I bet you feel this bankroll if I bump into you, oh yeah Chopper bullets flying errywhere, got them chopper bullets flying errywhere .223 chopper clip as long as Stacey Augmon., in the restaurant I order 2 fried lobsters Napkin on my Balmains, smokin' with lil mama Like fuck your baby daddy, his daddy should've worn a condom All these grams on me, all these bands on me Make her want to dance on me and put her hands on me Sprinter van on me, I got them Zans on me Driveway so damn long by the time I leave I'm damn asleep I'm the king of the trap, El Chapo Jr I'm the king of the trap, El Chapo Jr El Chapo Jr, El Chapo Jr, El Chapo Jr, El Chapo Jr I'm the king of the trap, El Chapo Jr Anywhere on the map, I bring it to you El Chapo Jr, El Chapo Jr, El Chapo Jr, I'ma bring it to ya I done put so many on I can't fall off, trying to see how much the West End Mall costs I might buy a Maybach and paint it all gold, buy a crib fill the rooms up with all stoves [?] pocket full of money [?] I feel like I just paid off the plug, drop the top off the whip, hell I receive High life nights so I woke up this evening, can't pimp me so don't you tip me Rims on the car make it six feet, so I can park the car wherever the blimps be I want a bird for my birthday, I drink syrup with no pancakes Yeah, bitches round my pool, I make them hoe look like my landscape Yeah, El Chapo J R R, multicolor A.R. rules Looking like I play ball, tell 'em keep they day jobs I'm the king of the trap, El Chapo Jr I'm the king of the trap, El Chapo Jr El Chapo Jr, El Chapo Jr, El Chapo Jr, El Chapo Jr I'm the king of the trap, El Chapo Jr Anywhere on the map, I bring it to you El Chapo Jr, El Chapo Jr, El Chapo Jr, I'ma bring it to ya ",
             title => "El Chapo Jr",
            album => "Trap-A-Velli Tre",
            year => 2015i64
        );
        let doc3 = doc!(
            lyrics => "[Verse 1: 2 Chainz] I am smoking on that gas, life should be on Cinemax Movie, Bought my boo bigger tits and a bigger ass Who he's, not I, I smoke strong, that Popeye Louie V's in my archives, black diamonds, apartheid Bread up and my top down On the block with a block out Hit ya ass with that block out Dope enough to go in yo nostrils I'll take ya girl and kid nap her Feed her to my mattress The skeleton in my closet It's probably dead ass rappers It's probably pussy ass niggas Don't try me I pull that trigga Got ya car note in my cup And your rent in my swisha That pussy so good I miss ya Head game's so vicious And all I get is cheese Like I'm taking pictures [Hook: Drake] I say fuck you, 'less I'm with' ya If I take you out of the picture I know real niggas won't miss ya No lie, no lie, no lie-ee-I-ee-I No lie, no lie, no lie-ee-I-ee-I Real niggas, say word, Ye ain't never told no lie Ye ain't never told no lie Real niggas, say word, Ye ain't never told no lie Ye ain't never told no lie Real niggas, stay true Ye ain't never told no lie Ye ain't never told no lie That's a thing I don't do Nah I just do it for the niggas That are trying to see a million 'fore they die Wattup [Verse 2: Drake] 2 Chainz and champagne You want true, that's true enough Forbes list like every year My office is my tour bus She came through, she brought food She got fucked, she knew wassup She think I'm the realest out And I say \"damn, that makes two of us\" Oh that look like what's her name Chances are there's what's her name Chances are, if she was acting up Then I fucked her once and never fucked again She could have a Grammy, I still treat her ass like a nominee Just need to know what that pussy like so one time it's fine with me Young as an intern, with money like I built the shit Streets talking that confirm Go ask them who just catch shit Stay keeping my cup full so I'm extra charged like a state tax Me an' Chainz go way back We don't talk shit, we just say facts (Yes Lord) [Hook: Drake] I say fuck you, 'less I'm with' ya If I take you out of the picture I know real niggas won't miss ya No lie, no lie, no lie-ee-I-ee-I No lie, no lie, no lie-ee-I-ee-I Real niggas, say word, Ye ain't never told no lie Ye ain't never told no lie Real niggas, say word, Ye ain't never told no lie Ye ain't never told no lie Real niggas, stay true Ye ain't never told no lie Ye ain't never told no lie That's a thing I don't do Nah I just do it for the niggas That are trying to see a million 'fore they die Wattup [Verse 3: 2 Chainz] Name a nigga that want some I'll out rap his ass I'll trap his ass Put his ass in a plastic bag with his trashy ass Take 'em out, bring 'em in Them whole things, 2Pac without a nose ring Thug Life, one wife, a mistress and a girlfriend I did what they say I wouldn't (go) Went where they say I couldn't (true) YSL belt buckle Ya'll niggas sure is looking Ya'll niggas sure is lucky 2 Chainz on my rugby Left hand on that steering wheel Right hand on that pussy [Hook: Drake] I say fuck you, 'less I'm with' ya If I take you out of the picture I know real niggas won't miss ya No lie, no lie, no lie-ee-I-ee-I No lie, no lie, no lie-ee-I-ee-I Real niggas, say word, Ye ain't never told no lie Ye ain't never told no lie Real niggas, say word, Ye ain't never told no lie Ye ain't never told no lie Real niggas, stay true Ye ain't never told no lie Ye ain't never told no lie That's a thing I don't do Nah I just do it for the niggas That are trying to see a million 'fore they die Wattup ",
            title => "No Lie",
            album => "Based On A T.R.U. Story",
            year => 2012i64
        );
        let doc4 = doc!(
            lyrics => "2 Chainz Mustard on the beat hoe I'm different, yeah I'm different I'm different, yeah I'm different I'm different, yeah I'm different Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Middle finger up to my competition I'm different, yeah I'm different I'm different, yeah I'm different I'm different, yeah I'm different Pull up to the scene with my ceiling missing Pull up to the scene, but my roof gone When I leave the scene, bet your boo gone And I beat the pussy like a new song 2 Chainz but I got me a few on Everything hot, skip lukewarm Tell shawty bust it open, Uncle Luke on Got the present for the present and a gift wrapping I don't feel good, but my trigger happy Bet the stripper happy, bet they wish they had me And I wish a nigga would, like a kitchen cabinet And me and you are cut from a different fabric I fucked her so good it's a bad habit Bitch sit down, you got a bad atti' Gave her the wrong number and a bad addy You ain't going nowhere like a bad navi Ass so big, I told her to look back at it Look back at it, look back at it Then I put a fat rabbit on the Craftmatic I am so high. Attic I am so high like a addict I'm different, yeah I'm different I'm different, yeah I'm different I'm different, yeah I'm different Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Middle finger up to my competition I'm different, yeah I'm different I'm different, yeah I'm different I'm different, yeah I'm different Pull up to the scene with my ceiling missing 2 Chainz got your girl on the celly And when I get off the celly I made her meet at the telly When she meet at the telly I put it straight in her belly When it go in her belly, it ain't shit you can tell me Hair long, money long Me and broke niggas we don't get along Hair long, money long Me and broke niggas we don't get along I paid a thousand dollars for my sneakers Ye told ya, a 100k for a feature Eee-err eee-err, sound of the bed Beat it up, beat it up, then I get some head Well I might get some head, then I beat it up I don't give a fuck, switch it up, nigga live it up Yeah it's going down, so get up Might valet park a Brinks truck I'm different, yeah I'm different I'm different, yeah I'm different I'm different, yeah I'm different Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Pull up to the scene with my ceiling missing Middle finger up to my competition I'm different, yeah I'm different I'm different, yeah I'm different I'm different, yeah I'm different Pull up to the scene with my ceiling missing",
            title => "I'm Different",
            album => "Based On A T.R.U. Story",
            year => 2012i64
        );
        let doc5 = doc!(
            lyrics => "[Intro] Last night I bought the supermodel Got head on the way home She left her other friends at the club Got home, and I fucked her with my chains on And that's ratchet huh? Her ass so big it look like she trying to walk backwards bruh (Woah bring it back) [Verse 1] Take your bitch like I'm the dog catcher Take your ass to the mall after Spa day, shawty Kill that pussy, pallbearer You from that RuPaul era I'm from that hell nah era Real niggas say true Real niggas ain't you I ride around with that yapper on me Gun clad with my Glock up on me Surround your ass with so many shots You'll be claustrophobic Crib so big a dinosaur can run through that shit I'm a shark, and you a tuna fish My paper up, got your girl with her ankles up Gangster boy [Bridge] She got her T-shirt and her panties on She trying to smell my cologne I can fuck her anywhere I want I even fuck her on the floor And thats ratchet huh? Her ass so big it look like she trying to walk backwards bruh (Woah bring it back) [Verse 2] I'm on top like a toupée You on the side like a toothache Box your ass, suitcase I'm real, you ain't Calamari, crab cakes My closet the size of your damn place You lookin' at a star I wish that they could add space I'm getting money, fast pace My hoes gave my cash straight He can't ball, castrate She the opposite of last place Do it, bust it open, slow it down, Robitussin My girl got a big ass Your girl got back pockets touching [Bridge] I want a dollar, I want everything Balls out, I let her hang Went shopping for a wedding ring Went on a double date with Molly and Mary Jane And thats ratchet huh? Her ass so big it look like she trying to walk backwards bruh [Verse 3] If you patch a weed that's ratchet My last album was classic Shawty on my dica Backwards spells acid She pop a P like a zany She'll use no hands and no panties She rock like nose candy You fuck with me, ain't no plan B I'm a D boy with a degree I sold dope in my momma's home My girl pussy deep (deep) So right there is my comfort zone Born alone, die alone Mud in my styrofoam She got on top of me like a stage, I said use my dick as a microphone ",
            title => "Mainstream Ratchet",
            album => "B.O.A.T.S. II: Me Time",
            year => 2013i64
        );
        let doc6 = doc!(
            lyrics => "[2 Chainz:] 2 Chainz [Quavo (2 Chainz):] Yeah, VIP my squad (yeah) Drop off all the gang (yeah) VIP, the lane (skrrt) VIP, the chain (squad) (Murda on the beat so it's not nice) VIP, my squad (yeah, squad) Drop off all the gang (yeah) VIP, the lane (skrrt, yeah) VIP, the chain Uh, yeah, baller alert Lil biddy bitch on caller alert (brrrrp) Uh, yeah, follow alert Go get the fire when I'm callin' 'em merked Uh, yeah, have to come first I whip the baby, the baby gon' birth (whip it) Uh, yeah, I bought a Claren (woo) I bought a 'Claren, didn't wanna buy percs, uh This shit bigger than you (hey) I'm taking on a new path (uh) Making the bitch take a bath (woo) Lil biddy bitch, do the math (yeah) Lil nigga, who are you? (who you, yeah) Must be bulletproof (brrrp) This shit bigger than you (it big) This shit bigger than you [2 Chainz:] Chain so big, should have came with a kick stand Fuck with me, I got a retain on a hit man (bop) Barely came up out the mud like quicksand (barely) I show you how to get millions, nigga, that's a mil plan, now Uh, yeah, ring the alarm Cartier bracelets on all of my arms Uh, yeah, halo my son In the wheelchair, and I still perform (Uh) I don't make excuses, you know that I'm hungry, I still got the juice Uh, you settle down like a Cleo, I settle down like a boost [Quavo (2 Chainz):] Uh, yeah, baller alert Lil biddy bitch on caller alert (brrrrp) Uh, yeah, follow alert Go get the fire when I'm callin' 'em merked Uh, yeah, have to come first I whip the baby, the baby gon' birth (whip it) Uh, yeah, I bought a Claren (woo) I bought a 'Claren, didn't wanna buy percs, uh This shit bigger than you (hey) I'm taking on a new path (uh) Making the bitch take a bath (woo) Lil biddy bitch, do the math (yeah) Lil nigga, who are you? (who you?) Must be bulletproof (brrrp) (bye) This shit bigger than you (it big) This shit bigger than you [Drake (Quavo):] Young champagne checkin' in, man, Tity Boi shit ringin' off 'Member I was on pre-paid, I would act like my shit was ringin' off 'Member shorty told me she thought the raps good but the singing's off Watch on Young Dro now, man, boioing shit blingin' off Where the racks at? (racks) All I know is they keep comin' to me like a flashback, nigga, what? (what?) Half a million out in Vegas, it ain't no blackjack, nigga, naw (naw) Quavo Sinatra, but we could never be the ratpack, nigga, naw [Quavo (2 Chainz):] Uh, yeah, baller alert Lil biddy bitch on caller alert (brrrrp) Uh, yeah, follow alert Go get the fire when I'm callin' 'em merked Uh, yeah, have to come first I whip the baby, the baby gon' birth (whip it) Uh, yeah, I bought a Claren (woo) I bought a 'Claren, didn't wanna buy percs, uh This shit bigger than you (hey) I'm taking on a new path (uh) Making the bitch take a bath (woo, tell 'em) Lil biddy bitch, do the math (yeah) Lil nigga, who are you? (Who you?) Must be bulletproof (brrrp) (bye) This shit bigger than you (it big) This shit bigger than you [2 Chainz:] Touchscreen on my cars, vintage one to two I just bought a watch that's plain like a Dickie suit (plain) I sip some red wine, and chased it with the '42 They asked me what I call millions, comin' soon Yeah, I just cashed out (yeah) (Uh) Ain't got time for a beef from a cash cow (tell 'em) When I was in juvie, I made 'em back out (back that ass up) It was 400 degrees, you would have passed out [Quavo (2 Chainz):] Uh, yeah, baller alert Lil biddy bitch on caller alert (brrrrp) Uh, yeah, follow alert Go get the fire when I'm callin' 'em merked (yeaaaah) Uh, yeah, have to come first I whip the baby, the baby gon' birth (whip it) Uh, yeah, I bought a Claren (woo) I bought a 'Claren, didn't wanna buy percs, uh This shit bigger than you (hey) I'm taking on a new path (uh) Making the bitch take a bath (woo) Lil biddy bitch, do the math (yeah) Lil nigga, who are you? (who you?) Must be bulletproof (brrrp) (bye) This shit bigger than you (it big) This shit bigger than you VIP my squad (yeah) Drop off all the gang (yeah) VIP, the lane (skrrt) VIP, the chain (squad) VIP, my squad (yeah) (yeah) Drop off all the gang (yeah) VIP, the lane (skrrt) VIP, the chain ",
            title => "Bigger Than You",
            album => "Bigger Than You",
            year => 2018i64
        );
        let mut bytes = 0;
        let mut writer = index.writer(50_000_000).unwrap();
        bytes += writer.add_document(doc);
        bytes += writer.add_document(doc2);
        bytes += writer.add_document(doc3);
        bytes += writer.add_document(doc4);
        bytes += writer.add_document(doc5);
        bytes += writer.add_document(doc6);

        println!("Indexed {} Bytes...", bytes);

        writer.commit().unwrap();
    }
}
