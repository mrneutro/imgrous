package it.unisa.di.soa2019.partitioning;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StopWords {
    private static StopWords ourInstance;
    private Set<String> stopWordList = new HashSet<>(1000);

    private StopWords() {
        String italian = "a abbastanza abbia abbiamo abbiano abbiate accidenti ad adesso affinche agl agli ahime ahimã¨ ahimè ai al alcuna alcuni alcuno all alla alle allo allora altre altri altrimenti altro altrove altrui anche ancora anni anno ansa anticipo assai attesa attraverso avanti avemmo avendo avente aver avere averlo avesse avessero avessi avessimo aveste avesti avete aveva avevamo avevano avevate avevi avevo avrai avranno avrebbe avrebbero avrei avremmo avremo avreste avresti avrete avrà avrò avuta avute avuti avuto basta ben bene benissimo berlusconi brava bravo buono c casa caso cento certa certe certi certo che chi chicchessia chiunque ci ciascuna ciascuno cima cinque cio cioe cioã¨ cioè circa citta città cittã ciã² ciò co codesta codesti codesto cogli coi col colei coll coloro colui come cominci comprare comunque con concernente conciliarsi conclusione consecutivi consecutivo consiglio contro cortesia cos cosa cosi cosã¬ così cui d da dagl dagli dai dal dall dalla dalle dallo dappertutto davanti degl degli dei del dell della delle dello dentro detto deve devo di dice dietro dire dirimpetto diventa diventare diventato dopo doppio dov dove dovra dovrà dovrã dovunque due dunque durante e ebbe ebbero ebbi ecc ecco ed effettivamente egli ella entrambi eppure era erano eravamo eravate eri ero esempio esse essendo esser essere essi ex fa faccia facciamo facciano facciate faccio facemmo facendo facesse facessero facessi facessimo faceste facesti faceva facevamo facevano facevate facevi facevo fai fanno farai faranno fare farebbe farebbero farei faremmo faremo fareste faresti farete farà farò fatto favore fece fecero feci fin finalmente finche fine fino forse forza fosse fossero fossi fossimo foste fosti fra frattempo fu fui fummo fuori furono futuro generale gente gia giacche giorni giorno giu già giã gli gliela gliele glieli glielo gliene governo grande grazie gruppo ha haha hai hanno ho i ie ieri il improvviso in inc indietro infatti inoltre insieme intanto intorno invece io l la lasciato lato lavoro le lei li lo lontano loro lui lungo luogo là lã ma macche magari maggior mai male malgrado malissimo mancanza marche me medesimo mediante meglio meno mentre mesi mezzo mi mia mie miei mila miliardi milioni minimi ministro mio modo molta molti moltissimo molto momento mondo mosto nazionale ne negl negli nei nel nell nella nelle nello nemmeno neppure nessun nessuna nessuno niente no noi nome non nondimeno nonostante nonsia nostra nostre nostri nostro novanta nove nulla nuovi nuovo o od oggi ogni ognuna ognuno oltre oppure ora ore osi ossia ottanta otto paese parecchi parecchie parecchio parte partendo peccato peggio per perche perchã¨ perchè perché percio perciã² perciò perfino pero persino persone perã² però piedi pieno piglia piu piuttosto piã¹ più po pochissimo poco poi poiche possa possedere posteriore posto potrebbe preferibilmente presa press prima primo principalmente probabilmente promesso proprio puo pure purtroppo puã² può qua qualche qualcosa qualcuna qualcuno quale quali qualunque quando quanta quante quanti quanto quantunque quarto quasi quattro quel quella quelle quelli quello quest questa queste questi questo qui quindi quinto realmente recente recentemente registrazione relativo riecco rispetto salvo sara sarai saranno sarebbe sarebbero sarei saremmo saremo sareste saresti sarete sarà sarã sarò scola scopo scorso se secondo seguente seguito sei sembra sembrare sembrato sembrava sembri sempre senza sette si sia siamo siano siate siete sig solito solo soltanto sono sopra soprattutto sotto spesso srl sta stai stando stanno starai staranno starebbe starebbero starei staremmo staremo stareste staresti starete starà starò stata state stati stato stava stavamo stavano stavate stavi stavo stemmo stessa stesse stessero stessi stessimo stesso steste stesti stette stettero stetti stia stiamo stiano stiate sto su sua subito successivamente successivo sue sugl sugli sui sul sull sulla sulle sullo suo suoi tale tali talvolta tanto te tempo terzo th ti titolo torino tra tranne tre trenta triplo troppo trovato tu tua tue tuo tuoi tutta tuttavia tutte tutti tutto uguali ulteriore ultimo un una uno uomo va vai vale vari varia varie vario verso vi via vicino visto vita voi volta volte vostra vostre vostri vostro";
        stopWordList.addAll(Arrays.asList(italian.split(" ")));
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