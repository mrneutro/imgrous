package it.unisa.di.soa2019.partitioning;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StopWords {
    private static StopWords ourInstance;
    private Set<String> stopWordList = new HashSet<>(1000);

    private StopWords() {
        String italian = "a abbastanza abbia abbiamo abbiano abbiate accidenti ad adesso affinche agl agli ahime ahimã¨ ahimè ai al alcuna alcuni alcuno all alla alle allo allora altre altri altrimenti altro altrove altrui anche ancora anni anno ansa anticipo assai attesa attraverso avanti avemmo avendo avente aver avere averlo avesse avessero avessi avessimo aveste avesti avete aveva avevamo avevano avevate avevi avevo avrai avranno avrebbe avrebbero avrei avremmo avremo avreste avresti avrete avrà avrò avuta avute avuti avuto basta ben bene benissimo berlusconi brava bravo buono c casa caso cento certa certe certi certo che chi chicchessia chiunque ci ciascuna ciascuno cima cinque cio cioe cioã¨ cioè circa citta città cittã ciã² ciò co codesta codesti codesto cogli coi col colei coll coloro colui come cominci comprare comunque con concernente conciliarsi conclusione consecutivi consecutivo consiglio contro cortesia cos cosa cosi cosã¬ così cui d da dagl dagli dai dal dall dalla dalle dallo dappertutto davanti degl degli dei del dell della delle dello dentro detto deve devo di dice dietro dire dirimpetto diventa diventare diventato dopo doppio dov dove dovra dovrà dovrã dovunque due dunque durante e ebbe ebbero ebbi ecc ecco ed effettivamente egli ella entrambi eppure era erano eravamo eravate eri ero esempio esse essendo esser essere essi ex fa faccia facciamo facciano facciate faccio facemmo facendo facesse facessero facessi facessimo faceste facesti faceva facevamo facevano facevate facevi facevo fai fanno farai faranno fare farebbe farebbero farei faremmo faremo fareste faresti farete farà farò fatto favore fece fecero feci fin finalmente finche fine fino forse forza fosse fossero fossi fossimo foste fosti fra frattempo fu fui fummo fuori furono futuro generale gente gia giacche giorni giorno giu già giã gli gliela gliele glieli glielo gliene governo grande grazie gruppo ha haha hai hanno ho i ie ieri il improvviso in inc indietro infatti inoltre insieme intanto intorno invece io l la lasciato lato lavoro le lei li lo lontano loro lui lungo luogo là lã ma macche magari maggior mai male malgrado malissimo mancanza marche me medesimo mediante meglio meno mentre mesi mezzo mi mia mie miei mila miliardi milioni minimi ministro mio modo molta molti moltissimo molto momento mondo mosto nazionale ne negl negli nei nel nell nella nelle nello nemmeno neppure nessun nessuna nessuno niente no noi nome non nondimeno nonostante nonsia nostra nostre nostri nostro novanta nove nulla nuovi nuovo o od oggi ogni ognuna ognuno oltre oppure ora ore osi ossia ottanta otto paese parecchi parecchie parecchio parte partendo peccato peggio per perche perchã¨ perchè perché percio perciã² perciò perfino pero persino persone perã² però piedi pieno piglia piu piuttosto piã¹ più po pochissimo poco poi poiche possa possedere posteriore posto potrebbe preferibilmente presa press prima primo principalmente probabilmente promesso proprio puo pure purtroppo puã² può qua qualche qualcosa qualcuna qualcuno quale quali qualunque quando quanta quante quanti quanto quantunque quarto quasi quattro quel quella quelle quelli quello quest questa queste questi questo qui quindi quinto realmente recente recentemente registrazione relativo riecco rispetto salvo sara sarai saranno sarebbe sarebbero sarei saremmo saremo sareste saresti sarete sarà sarã sarò scola scopo scorso se secondo seguente seguito sei sembra sembrare sembrato sembrava sembri sempre senza sette si sia siamo siano siate siete sig solito solo soltanto sono sopra soprattutto sotto spesso srl sta stai stando stanno starai staranno starebbe starebbero starei staremmo staremo stareste staresti starete starà starò stata state stati stato stava stavamo stavano stavate stavi stavo stemmo stessa stesse stessero stessi stessimo stesso steste stesti stette stettero stetti stia stiamo stiano stiate sto su sua subito successivamente successivo sue sugl sugli sui sul sull sulla sulle sullo suo suoi tale tali talvolta tanto te tempo terzo th ti titolo torino tra tranne tre trenta triplo troppo trovato tu tua tue tuo tuoi tutta tuttavia tutte tutti tutto uguali ulteriore ultimo un una uno uomo va vai vale vari varia varie vario verso vi via vicino visto vita voi volta volte vostra vostre vostri vostro";
        String english = "a as able about above according accordingly across actually after afterwards again against aint all allow allows almost alone along already also although always am among amongst an and another any anybody anyhow anyone anything anyway anyways anywhere apart appear appreciate appropriate are arent around as aside ask asking associated at available away awfully be became because become becomes becoming been before beforehand behind being believe below beside besides best better between beyond both brief but by cmon cs came can cant cannot cant cause causes certain certainly changes clearly co com come comes concerning consequently consider considering contain containing contains corresponding could couldnt course currently definitely described despite did didnt different do does doesnt doing dont done down downwards during each edu eg eight either else elsewhere enough entirely especially et etc even ever every everybody everyone everything everywhere ex exactly example except far few ff fifth first five followed following follows for former formerly forth four from further furthermore get gets getting given gives go goes going gone got gotten greetings had hadnt happens hardly has hasnt have havent having he hes hello help hence her here heres hereafter hereby herein hereupon hers herself hi him himself his hither hopefully how howbeit however i id ill im ive ie if ignored immediate in inasmuch inc indeed indicate indicated indicates inner insofar instead into inward is isnt it itd itll its its itself just keep keeps kept know knows known last lately later latter latterly least less lest let lets like liked likely little look looking looks ltd mainly many may maybe me mean meanwhile merely might more moreover most mostly much must my myself name namely nd near nearly necessary need needs neither never nevertheless new next nine no nobody non none noone nor normally not nothing novel now nowhere obviously of off often oh ok okay old on once one ones only onto or other others otherwise ought our ours ourselves out outside over overall own particular particularly per perhaps placed please plus possible presumably probably provides que quite qv rather rd re really reasonably regarding regardless regards relatively respectively right said same saw say saying says second secondly see seeing seem seemed seeming seems seen self selves sensible sent serious seriously seven several shall she should shouldnt since six so some somebody somehow someone something sometime sometimes somewhat somewhere soon sorry specified specify specifying still sub such sup sure ts take taken tell tends th than thank thanks thanx that thats thats the their theirs them themselves then thence there theres thereafter thereby therefore therein theres thereupon these they theyd theyll theyre theyve think third this thorough thoroughly those though three through throughout thru thus to together too took toward towards tried tries truly try trying twice two un under unfortunately unless unlikely until unto up upon us use used useful uses using usually value various very via viz vs want wants was wasnt way we wed well were weve welcome well went were werent what whats whatever when whence whenever where wheres whereafter whereas whereby wherein whereupon wherever whether which while whither who whos whoever whole whom whose why will willing wish with within without wont wonder would would wouldnt yes yet you youd youll youre youve your yours yourself yourselves zero";
        String spanish = "a al algo algunas algunos ante antes como con contra cual cuando de del desde donde durante e el ella ellas ellos en entre era erais eran eras eres es esa esas ese eso esos esta estaba estabais estaban estabas estad estada estadas estado estados estamos estando estar estaremos estará estarán estarás estaré estaréis estaría estaríais estaríamos estarían estarías estas este estemos esto estos estoy estuve estuviera estuvierais estuvieran estuvieras estuvieron estuviese estuvieseis estuviesen estuvieses estuvimos estuviste estuvisteis estuviéramos estuviésemos estuvo está estábamos estáis están estás esté estéis estén estés fue fuera fuerais fueran fueras fueron fuese fueseis fuesen fueses fui fuimos fuiste fuisteis fuéramos fuésemos ha habida habidas habido habidos habiendo habremos habrá habrán habrás habré habréis habría habríais habríamos habrían habrías habéis había habíais habíamos habían habías han has hasta hay haya hayamos hayan hayas hayáis he hemos hube hubiera hubierais hubieran hubieras hubieron hubiese hubieseis hubiesen hubieses hubimos hubiste hubisteis hubiéramos hubiésemos hubo la las le les lo los me mi mis mucho muchos muy más mí mía mías mío míos nada ni no nos nosotras nosotros nuestra nuestras nuestro nuestros o os otra otras otro otros para pero poco por porque que quien quienes qué se sea seamos sean seas seremos será serán serás seré seréis sería seríais seríamos serían serías seáis sido siendo sin sobre sois somos son soy su sus suya suyas suyo suyos sí también tanto te tendremos tendrá tendrán tendrás tendré tendréis tendría tendríais tendríamos tendrían tendrías tened tenemos tenga tengamos tengan tengas tengo tengáis tenida tenidas tenido tenidos teniendo tenéis tenía teníais teníamos tenían tenías ti tiene tienen tienes todo todos tu tus tuve tuviera tuvierais tuvieran tuvieras tuvieron tuviese tuvieseis tuviesen tuvieses tuvimos tuviste tuvisteis tuviéramos tuviésemos tuvo tuya tuyas tuyo tuyos tú un una uno unos vosotras vosotros vuestra vuestras vuestro vuestros y ya yo él éramos ";
        String russian = "c а алло без белый близко более больше большой будем будет будете будешь будто буду будут будь бы бывает бывь был была были было быть в важная важное важные важный вам вами вас ваш ваша ваше ваши вверх вдали вдруг ведь везде вернуться весь вечер взгляд взять вид видел видеть вместе вне вниз внизу во вода война вокруг вон вообще вопрос восемнадцатый восемнадцать восемь восьмой вот впрочем времени время все всегда всего всем всеми всему всех всею всю всюду вся всё второй вы выйти г где главный глаз говорил говорит говорить год года году голова голос город да давать давно даже далекий далеко дальше даром дать два двадцатый двадцать две двенадцатый двенадцать дверь двух девятнадцатый девятнадцать девятый девять действительно дел делал делать делаю дело день деньги десятый десять для до довольно долго должен должно должный дом дорога друг другая другие других друго другое другой думать душа е его ее ей ему если есть еще ещё ею её ж ждать же жена женщина жизнь жить за занят занята занято заняты затем зато зачем здесь земля знать значит значить и иди идти из или им имеет имел именно иметь ими имя иногда их к каждая каждое каждые каждый кажется казаться как какая какой кем книга когда кого ком комната кому конец конечно которая которого которой которые который которых кроме кругом кто куда лежать лет ли лицо лишь лучше любить люди м маленький мало мать машина между меля менее меньше меня место миллионов мимо минута мир мира мне много многочисленная многочисленное многочисленные многочисленный мной мною мог могу могут мож может можно можхо мои мой мор москва мочь моя моё мы на наверху над надо назад наиболее найти наконец нам нами народ нас начала начать наш наша наше наши не него недавно недалеко нее ней некоторый нельзя нем немного нему непрерывно нередко несколько нет нею неё ни нибудь ниже низко никакой никогда никто никуда ним ними них ничего ничто но новый нога ночь ну нужно нужный нх о об оба обычно один одиннадцатый одиннадцать однажды однако одного одной оказаться окно около он она они оно опять особенно остаться от ответить отец откуда отовсюду отсюда очень первый перед писать плечо по под подойди подумать пожалуйста позже пойти пока пол получить помнить понимать понять пор пора после последний посмотреть посреди потом потому почему почти правда прекрасно при про просто против процентов путь пятнадцатый пятнадцать пятый пять работа работать раз разве рано раньше ребенок решить россия рука русский ряд рядом с сам сама сами самим самими самих само самого самой самом самому саму самый свет свое своего своей свои своих свой свою сделать сеаой себе себя сегодня седьмой сейчас семнадцатый семнадцать семь сидеть сила сих сказал сказала сказать сколько слишком слово случай смотреть сначала снова со собой собою советский совсем спасибо спросить сразу стал старый стать стол сторона стоять страна суть считать т та так такая также таки такие такое такой там твои твой твоя твоё те тебе тебя тем теми теперь тех то тобой тобою товарищ тогда того тоже только том тому тот тою третий три тринадцатый тринадцать ту туда тут ты тысяч у увидеть уж уже улица уметь утро хороший хорошо хотел хотеть хоть хотя хочешь час часто часть чаще чего человек чем чему через четвертый четыре четырнадцатый четырнадцать что чтоб чтобы чуть шестнадцатый шестнадцать шестой шесть эта эти этим этими этих это этого этой этом этому этот эту я являюсь";
        stopWordList.addAll(Arrays.asList(italian.split(" ")));
        stopWordList.addAll(Arrays.asList(english.split(" ")));
        stopWordList.addAll(Arrays.asList(spanish.split(" ")));
        stopWordList.addAll(Arrays.asList(russian.split(" ")));
    }

    public static StopWords getInstance() {
        if (ourInstance == null) {
            ourInstance = new StopWords();
        }
        return ourInstance;
    }

    public boolean isStopWord(final String word) {
        return stopWordList.contains(word);
    }
}
