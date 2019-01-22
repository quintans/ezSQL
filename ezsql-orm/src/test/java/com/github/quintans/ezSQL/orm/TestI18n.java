package com.github.quintans.ezSQL.orm;

import static com.github.quintans.ezSQL.orm.app.mappings.virtual.TAuthor.*;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Test;

import com.github.quintans.ezSQL.orm.app.mappings.virtual.Author;
import com.github.quintans.ezSQL.orm.app.mappings.virtual.Book;
import com.github.quintans.ezSQL.orm.app.mappings.virtual.TBook;
import com.github.quintans.ezSQL.orm.app.mappings.virtual.TBook18;

/**
 * Unit test for simple App.
 */
public class TestI18n extends TestBootstrap {

    @Test
    public void testI18n() {
        Collection<Book> books = db.query(TBook.T_BOOK).all()
                .outer(TBook.A_I18N).fetch()
                .orderBy(TBook18.C_NAME).asc()
                .list(Book.class, false);
        
        dumpCollection(books);

        for (Book book : books) {
            assertTrue("Book.name is null", book.getI18n().getName() != null);
        }
    }

    @Test
    public void testI18nWhere() throws Exception {
        try {
            Collection<Book> books = db.query(TBook.T_BOOK).all()
                    .inner(TBook.A_I18N).on(TBook18.C_NAME.like("%SQL%")).fetch()
                    .where(TBook.C_PRICE.lt(20.0D))
                    .list(Book.class, false);
            
            dumpCollection(books);

            for (Book book : books) {
                assertTrue("Book.name is null", book.getI18n().getName() != null);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Test
    public void testI18nAssociation() throws Exception {
        try {
            db.languague = "es";

            Collection<Author> authors = db.query(T_AUTHOR).all()
                    .inner(T_AUTHOR.A_BOOKS, TBook.A_I18N).fetch()
                    .orderBy(TBook18.C_NAME).desc()
                    .list(Author.class, false);
            
            dumpCollection(authors);
            assertTrue("Authors is NOT empty", authors.isEmpty());

            System.out.println();
            authors = db.query(T_AUTHOR).all()
                    .outer(T_AUTHOR.A_BOOKS, TBook.A_I18N).fetch()
                    .orderBy(TBook18.C_NAME).asc()
                    .list(Author.class, true);
            
            dumpCollection(authors);
            for (Author author : authors) {
                if (author.getBooks() != null) {
                    for (Book book : author.getBooks()) {
                        assertTrue("Author.Book is NOT null", book.getI18n() == null);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testI18nAssociationConditioned() throws Exception {
        try {
            db.languague = "en";
            Collection<Author> result = db.query(T_AUTHOR).all()
                    .inner(T_AUTHOR.A_BOOKS).on(TBook.C_PRICE.lt(20.0))
                    .inner(TBook.A_I18N).on(TBook18.C_NAME.like("%SQL%").not()).fetch()
                    .list(Author.class, false);
            
            dumpCollection(result);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

}